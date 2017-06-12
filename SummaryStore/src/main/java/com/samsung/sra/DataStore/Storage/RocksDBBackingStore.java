package com.samsung.sra.DataStore.Storage;

import com.samsung.sra.DataStore.LandmarkWindow;
import com.samsung.sra.DataStore.SummaryWindow;
import com.samsung.sra.DataStore.Utilities;
import org.apache.commons.lang.NotImplementedException;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class RocksDBBackingStore extends BackingStore {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBBackingStore.class);

    private final RocksDB rocksDB;
    private final Options rocksDBOptions;
    private final WriteOptions rocksDBWriteOptions;
    private final long cacheSizePerStream;
    /**
     * Map streamID -> windowID -> window. Caching is exclusive and each stream's cache holds a suffix of the stream
     * (its newest windows) of length <= cacheSizePerStream
     */
    private final ConcurrentHashMap<Long, ConcurrentSkipListMap<Long, SummaryWindow>> cache;

    /**
     * @param rocksPath  on-disk path
     * @param cacheSizePerStream  number of elements per stream to cache in main memory. Set to 0 to disable caching
     * @throws BackingStoreException  wrapping RocksDBException
     */
    public RocksDBBackingStore(String rocksPath, long cacheSizePerStream) throws BackingStoreException {
        this.cacheSizePerStream = cacheSizePerStream;
        cache = cacheSizePerStream > 0 ? new ConcurrentHashMap<>() : null;
        rocksDBOptions = new Options()
                .setCreateIfMissing(true)
                .createStatistics()
                .setStatsDumpPeriodSec(300) // seconds
                .setMaxBackgroundCompactions(10) // number of threads
                .setAllowConcurrentMemtableWrite(true)
                .setMaxBytesForLevelBase(512L * 1024 * 1024)
                .setDbWriteBufferSize(4L * 1024 * 1024 * 1024)
                //.setCompressionType(CompressionType.NO_COMPRESSION)
                //.setMemTableConfig(new VectorMemTableConfig())
                .setTableFormatConfig(new BlockBasedTableConfig()
                        .setBlockCacheSize(8L * 1024 * 1024 * 1024)
                        .setFilter(new BloomFilter()))
                .setMaxOpenFiles(-1);
        rocksDBWriteOptions = new WriteOptions()
                .setDisableWAL(true);
        try {
            rocksDB = RocksDB.open(rocksDBOptions, rocksPath);
        } catch (RocksDBException e) {
            throw new BackingStoreException(e);
        }
    }

    static {
        RocksDB.loadLibrary();
    }

    private static final int KEY_SIZE = 16;

    /**
     * RocksDB key = <streamID, windowID>. Since we ensure windowIDs are assigned in increasing
     * order, this lays out data in temporal order within streams
     */
    private static byte[] getRocksDBKey(long streamID, long windowID) {
        byte[] keyArray = new byte[KEY_SIZE];
        Utilities.longToByteArray(streamID, keyArray, 0);
        Utilities.longToByteArray(windowID, keyArray, 8);
        return keyArray;
    }

    private static long parseRocksDBKeyStreamID(byte[] keyArray) {
        return Utilities.byteArrayToLong(keyArray, 0);
    }

    private static long parseRocksDBKeyWindowID(byte[] keyArray) {
        return Utilities.byteArrayToLong(keyArray, 8);
    }

    /**
     * Attempt to insert value into cache, evicting an older entry if necessary and possible. Insert might fail if
     * cache is full and all entries already in cache are newer. Returns true iff insert succeeded.
     */
    private boolean insertIntoCache(ConcurrentSkipListMap<Long, SummaryWindow> streamCache, StreamWindowManager windowManager,
                                 long swid, SummaryWindow window) throws RocksDBException {
        assert streamCache != null;
        if (streamCache.size() >= cacheSizePerStream && streamCache.firstKey() > swid) {
            // cache is full && all entries in cache are newer than this window
            return false;
        } else {
            if (streamCache.size() >= cacheSizePerStream && !streamCache.containsKey(swid)) { // evict oldest
                long evictedSWID = streamCache.firstKey();
                SummaryWindow evictedWindow = streamCache.remove(evictedSWID);
                byte[] evictedKey = getRocksDBKey(windowManager.streamID, evictedSWID);
                byte[] evictedValue = windowManager.serializeSummaryWindow(evictedWindow);
                rocksDB.put(rocksDBWriteOptions, evictedKey, evictedValue);
            }
            streamCache.put(swid, window);
            return true;
        }
    }

    private SummaryWindow getAndOrDeleteSummaryWindow(StreamWindowManager windowManagar, long swid, boolean delete)
            throws BackingStoreException {
        ConcurrentSkipListMap<Long, SummaryWindow> streamCache;
        if (cache == null) {
            streamCache = null;
        } else {
            streamCache = cache.get(windowManagar.streamID);
            if (streamCache == null) cache.put(windowManagar.streamID, streamCache = new ConcurrentSkipListMap<>());
        }

        SummaryWindow window = streamCache != null ? streamCache.get(swid) : null;
        if (window != null) { // cache hit
            if (delete) streamCache.remove(swid);
            return window;
        } else { // either no cache or cache miss; read-through from RocksDB
            byte[] rocksKey = getRocksDBKey(windowManagar.streamID, swid);
            try {
                byte[] rocksValue = rocksDB.get(rocksKey);
                window = windowManagar.deserializeSummaryWindow(rocksValue);
                if (delete) {
                    rocksDB.delete(rocksKey);
                } else {
                    if (streamCache != null) {
                        boolean inserted = insertIntoCache(streamCache, windowManagar, swid, window);
                        if (inserted) {
                            rocksDB.delete(rocksKey);
                        }
                    }
                }
                return window;
            } catch (RocksDBException e) {
                throw new BackingStoreException(e);
            }
        }
    }

    @Override
    SummaryWindow getSummaryWindow(StreamWindowManager windowManager, long swid) throws BackingStoreException {
        return getAndOrDeleteSummaryWindow(windowManager, swid, false);
    }

    @Override
    SummaryWindow deleteSummaryWindow(StreamWindowManager windowManager, long swid) throws BackingStoreException {
        return getAndOrDeleteSummaryWindow(windowManager, swid, true);
    }

    /** Iterate over and return all summary windows in RocksDB overlapping the time-range given in the constructor */
    private class OverlappingRocksIterator implements Iterator<SummaryWindow> {
        private final RocksIterator rocksIterator;
        private SummaryWindow nextWindow;

        private final StreamWindowManager windowManager;
        private final long t0, t1;

        private OverlappingRocksIterator(StreamWindowManager windowManager, long t0, long t1) throws RocksDBException {
            this.windowManager = windowManager;
            this.t0 = t0;
            this.t1 = t1;

            // Note that Stream.query() ensures stream (1) is non-empty, (2) time interval [T0, T1] fully covers [t0, t1]
            rocksIterator = rocksDB.newIterator();
            rocksIterator.seek(getRocksDBKey(windowManager.streamID, t0));
            assert rocksIterator.isValid();
            nextWindow = readFromRocksIterator();
            if (nextWindow == null) { // only one window in stream and t0 > T0
                throw new NotImplementedException("this case not yet implemented, needs code restructure"); // FIXME
            }
            assert nextWindow.ts >= t0;
            /* rocksIterator now points to the first window with start timestamp >= t0. If timestamp == t0, we only
             * need to return this window and its successors. If timestamp > t0, we also need to return the window just
             * before this one (which is the last window with start timestamp < t0), so we call iterator.prev() */
            if (nextWindow.ts > t0) {
                rocksIterator.prev();
                nextWindow = readFromRocksIterator();
                assert nextWindow != null;
            }
        }

        private SummaryWindow readFromRocksIterator() {
            if (rocksIterator.isValid()) {
                byte[] key = rocksIterator.key();
                if (key.length == KEY_SIZE) {
                    long streamID = parseRocksDBKeyStreamID(key), ts = parseRocksDBKeyWindowID(key);
                    if (streamID == windowManager.streamID && ts <= t1) {
                        return windowManager.deserializeSummaryWindow(rocksIterator.value());
                    }
                }
            }
            // at least one of the "iterator is valid" conditions must have failed
            rocksIterator.close();
            return null;
        }

        @Override
        public boolean hasNext() {
            return nextWindow != null;
        }

        @Override
        public SummaryWindow next() {
            SummaryWindow ret = nextWindow;
            rocksIterator.next();
            nextWindow = readFromRocksIterator();
            return ret;
        }
    }

    @Override
    Stream<SummaryWindow> getSummaryWindowsOverlapping(StreamWindowManager windowManager, long t0, long t1)
            throws BackingStoreException {
        try {
            Iterator<SummaryWindow> iterator = new OverlappingRocksIterator(windowManager, t0, t1);
            Stream<SummaryWindow> rocksWindows =  StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
            ConcurrentSkipListMap<Long, SummaryWindow> streamCache = cache == null ? null
                    : cache.get(windowManager.streamID);
            if (streamCache != null && !streamCache.isEmpty() && t0 <= streamCache.lastKey() && t1 >= streamCache.firstKey()) {
                Long l = streamCache.floorKey(t0);
                Long r = streamCache.higherKey(t1);
                if (l == null) {
                    l = streamCache.firstKey();
                }
                if (r == null) {
                    r = streamCache.lastKey() + 1;
                }
                Stream<SummaryWindow> cacheWindows = streamCache.subMap(l, true, r, false).values().stream();
                return Stream.concat(rocksWindows, cacheWindows);
            } else {
                return rocksWindows;
            }
        } catch (RocksDBException e) {
            throw new BackingStoreException(e);
        }
    }

    @Override
    long getNumSummaryWindows(StreamWindowManager windowManager) {
        try (RocksIterator iter = rocksDB.newIterator()){
            iter.seek(getRocksDBKey(windowManager.streamID, 0L));
            long ct = 0;
            while (iter.isValid() && parseRocksDBKeyStreamID(iter.key()) == windowManager.streamID) {
                ++ct;
                iter.next();
            }
            if (cache != null && cache.containsKey(windowManager.streamID)) {
                ct += cache.get(windowManager.streamID).size();
            }
            return ct;
        }
    }

    @Override
    void putSummaryWindow(StreamWindowManager windowManager, long swid, SummaryWindow window) throws BackingStoreException {
        try {
            boolean insertedIntoCache = false;
            if (cache != null) {
                ConcurrentSkipListMap<Long, SummaryWindow> streamCache = cache.get(windowManager.streamID);
                if (streamCache == null) cache.put(windowManager.streamID, streamCache = new ConcurrentSkipListMap<>());
                insertedIntoCache = insertIntoCache(streamCache, windowManager, swid, window);
            }
            if (!insertedIntoCache) {
                byte[] key = getRocksDBKey(windowManager.streamID, swid);
                byte[] value = windowManager.serializeSummaryWindow(window);
                rocksDB.put(rocksDBWriteOptions, key, value);
            }
        } catch (RocksDBException e) {
            throw new BackingStoreException(e);
        }
    }

    /*@Override
    public void warmupCache(Map<Long, StreamManager> streamManagers) throws RocksDBException {
        if (cache == null) return;

        RocksIterator iter = null;
        try {
            iter = rocksDB.newIterator();
            for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                byte[] keyArray = iter.key();
                if (keyArray.length != KEY_SIZE) continue; // ignore metadataSpecialKey
                long streamID = parseRocksDBKeyStreamID(keyArray);
                StreamManager streamManager = streamManagers.get(streamID);
                assert streamManager != null;
                ConcurrentHashMap<Long, SummaryWindow> streamCache = cache.get(streamID);
                if (streamCache == null) {
                    cache.put(streamID, streamCache = new ConcurrentHashMap<>());
                } else if (streamCache.size() >= cacheSizePerStream) {
                    continue;
                }
                long swid = parseRocksDBKeyWindowID(keyArray);
                SummaryWindow window = streamManager.deserializeSummaryWindow(iter.value());
                streamCache.put(swid, window);
            }
        } finally {
            if (iter != null) iter.dispose();
        }
    }*/

    @Override
    public void flushToDisk(StreamWindowManager windowManager) throws BackingStoreException {
        flushLandmarksToDisk(windowManager);
        if (cache == null) return;
        Map<Long, SummaryWindow> streamCache = cache.get(windowManager.streamID);
        if (streamCache != null) {
            for (Map.Entry<Long, SummaryWindow> entry: streamCache.entrySet()) {
                long swid = entry.getKey();
                SummaryWindow window = entry.getValue();
                byte[] rocksKey = getRocksDBKey(windowManager.streamID, swid);
                byte[] rocksValue = windowManager.serializeSummaryWindow(window);
                try {
                    rocksDB.put(rocksKey, rocksValue);
                } catch (RocksDBException e) {
                    throw new BackingStoreException(e);
                }
            }
        }
    }

    /* **** FIXME: Landmark cache has unbounded size **** */

    private static final int LANDMARK_KEY_SIZE = 17;

    private static byte[] getLandmarkRocksKey(long streamID, long lwid) {
        byte[] keyArray = new byte[LANDMARK_KEY_SIZE];
        keyArray[0] = 'L';
        Utilities.longToByteArray(streamID, keyArray, 1);
        Utilities.longToByteArray(lwid, keyArray, 9);
        return keyArray;
    }

    private ConcurrentHashMap<Long, ConcurrentHashMap<Long, LandmarkWindow>> landmarkCache = new ConcurrentHashMap<>();

    private void flushLandmarksToDisk(StreamWindowManager windowManager) throws BackingStoreException {
        long streamID = windowManager.streamID;
        Map<Long, LandmarkWindow> streamMap = landmarkCache.get(streamID);
        if (streamMap == null) return;
        for (Map.Entry<Long, LandmarkWindow> windowEntry: streamMap.entrySet()) {
            long lwid = windowEntry.getKey();
            LandmarkWindow window = windowEntry.getValue();
            try {
                rocksDB.put(getLandmarkRocksKey(streamID, lwid), window.serialize());
            } catch (RocksDBException e) {
                throw new BackingStoreException(e);
            }
        }
    }

    @Override
    LandmarkWindow getLandmarkWindow(StreamWindowManager windowManager, long lwid) throws BackingStoreException {
        Map<Long, LandmarkWindow> streamMap = landmarkCache.get(windowManager.streamID);
        if (streamMap != null && streamMap.containsKey(lwid)) {
            return streamMap.get(lwid);
        } else {
            byte[] bytes;
            try {
                bytes = rocksDB.get(getLandmarkRocksKey(windowManager.streamID, lwid));
            } catch (RocksDBException e) {
                throw new BackingStoreException(e);
            }
            return LandmarkWindow.deserialize(bytes);
        }
    }

    @Override
    void putLandmarkWindow(StreamWindowManager windowManager, long lwid, LandmarkWindow window) {
        ConcurrentHashMap<Long, LandmarkWindow> stream = landmarkCache.get(windowManager.streamID);
        if (stream == null) {
            landmarkCache.put(windowManager.streamID, (stream = new ConcurrentHashMap<>()));
        }
        stream.put(lwid, window);
    }

    @Override
    void printWindowState(StreamWindowManager windowManager) throws BackingStoreException {
        System.out.println("stream " + windowManager.streamID + ":");
        System.out.println("\tuncached summary windows:");
        try (RocksIterator iter = rocksDB.newIterator()) {
            iter.seek(getRocksDBKey(windowManager.streamID, 0L));
            while (iter.isValid() && parseRocksDBKeyStreamID(iter.key()) == windowManager.streamID) {
                System.out.println("\t\t" + windowManager.deserializeSummaryWindow(iter.value()));
                iter.next();
            }
            if (cache != null && cache.containsKey(windowManager.streamID)) {
                System.out.println("\tcached summary windows:");
                for (SummaryWindow window: cache.get(windowManager.streamID).values()) {
                    System.out.println("\t\t" + window);
                }
            }
            // TODO: landmarks
        }
    }

    @Override
    public void close() throws BackingStoreException {
        if (rocksDB != null) {
            rocksDB.close();
        }
        rocksDBOptions.close();
        logger.info("rocksDB closed");
    }
}
