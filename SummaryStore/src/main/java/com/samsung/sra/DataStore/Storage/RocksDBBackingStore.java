package com.samsung.sra.DataStore.Storage;

import com.samsung.sra.DataStore.LandmarkWindow;
import com.samsung.sra.DataStore.SummaryWindow;
import com.samsung.sra.DataStore.Utilities;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class RocksDBBackingStore extends BackingStore {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBBackingStore.class);

    private final RocksDB rocksDB;
    private final Options rocksDBOptions;
    private final long cacheSizePerStream;
    // map streamID -> windowID -> window
    private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, SummaryWindow>> cache;

    /**
     * @param rocksPath  on-disk path
     * @param cacheSizePerStream  number of elements per stream to cache in main memory. Set to 0 to disable caching
     * @throws BackingStoreException  wrapping RocksDBException
     */
    public RocksDBBackingStore(String rocksPath, long cacheSizePerStream) throws BackingStoreException {
        this.cacheSizePerStream = cacheSizePerStream;
        cache = cacheSizePerStream > 0 ? new ConcurrentHashMap<>() : null;
        rocksDBOptions = new Options().setCreateIfMissing(true);
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
    private byte[] getRocksDBKey(long streamID, long windowID) {
        byte[] keyArray = new byte[KEY_SIZE];
        Utilities.longToByteArray(streamID, keyArray, 0);
        Utilities.longToByteArray(windowID, keyArray, 8);
        return keyArray;
    }

    private long parseRocksDBKeyStreamID(byte[] keyArray) {
        return Utilities.byteArrayToLong(keyArray, 0);
    }

    private long parseRocksDBKeyWindowID(byte[] keyArray) {
        return Utilities.byteArrayToLong(keyArray, 8);
    }

    /**
     * Insert value into cache, evicting another cached entry if necessary
     */
    private void insertIntoCache(ConcurrentHashMap<Long, SummaryWindow> streamCache, StreamWindowManager windowManager,
                                 long swid, SummaryWindow window) throws RocksDBException {
        assert streamCache != null;
        if (streamCache.size() >= cacheSizePerStream) { // evict something
            Map.Entry<Long, SummaryWindow> evicted = streamCache.entrySet().iterator().next();
            byte[] evictedKey = getRocksDBKey(windowManager.streamID, evicted.getKey());
            byte[] evictedValue = windowManager.serializeSummaryWindow(evicted.getValue());
            rocksDB.put(evictedKey, evictedValue);
        }
        streamCache.put(swid, window);
    }

    private SummaryWindow getAndOrDeleteSummaryWindow(StreamWindowManager windowManagar, long swid, boolean delete)
            throws BackingStoreException {
        ConcurrentHashMap<Long, SummaryWindow> streamCache;
        if (cache == null) {
            streamCache = null;
        } else {
            streamCache = cache.get(windowManagar.streamID);
            if (streamCache == null) cache.put(windowManagar.streamID, streamCache = new ConcurrentHashMap<>());
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
                    rocksDB.remove(rocksKey);
                }
                if (streamCache != null) {
                    insertIntoCache(streamCache, windowManagar, swid, window);
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

    /** Iterate over and return all windows overlapping the time-range given in the constructor */
    private class OverlappingSWIterator implements Iterator<SummaryWindow> {
        private final RocksIterator rocksIterator;
        // comment in constructor explains logic
        private SummaryWindow headWindow, nextWindow;

        private final StreamWindowManager windowManager;
        private final long t0, t1;

        private OverlappingSWIterator(StreamWindowManager windowManager, long t0, long t1) throws RocksDBException {
            this.windowManager = windowManager;
            this.t0 = t0;
            this.t1 = t1;

            rocksIterator = rocksDB.newIterator();
            rocksIterator.seek(getRocksDBKey(windowManager.streamID, t0));
            /* rocksIterator now points to the first window with start timestamp >= t0. If timestamp == t0, we only
             * need to return this window and its successors. If timestamp > t0, we also need to return the window just
             * before this one (which is the last window with start timestamp < t0); we store that window in "headWindow"
             */
            nextWindow = readFromRocksIterator();
            if (nextWindow != null && nextWindow.ts > t0 && nextWindow.prevTS != -1) {
                headWindow = windowManager.deserializeSummaryWindow(
                        rocksDB.get(getRocksDBKey(windowManager.streamID, nextWindow.prevTS)));
            } else {
                headWindow = null;
            }
        }

        private SummaryWindow readFromRocksIterator() {
            if (rocksIterator.isValid()) {
                byte[] key = rocksIterator.key();
                if (key.length == KEY_SIZE) {
                    long streamID = parseRocksDBKeyStreamID(key), ts = parseRocksDBKeyWindowID(key);
                    if (streamID == windowManager.streamID) {
                        assert ts >= t0;
                        if (ts <= t1) {
                            return windowManager.deserializeSummaryWindow(rocksIterator.value());
                        }
                    }
                }
            }
            // at least one of the "iterator is valid" conditions must have failed
            rocksIterator.dispose();
            return null;
        }

        @Override
        public boolean hasNext() {
            return headWindow != null || nextWindow != null;
        }

        @Override
        public SummaryWindow next() {
            if (headWindow != null) {
                SummaryWindow ret = headWindow;
                headWindow = null;
                return ret;
            } else {
                SummaryWindow ret = nextWindow;
                rocksIterator.next();
                nextWindow = readFromRocksIterator();
                return ret;
            }
        }
    }

    @Override
    Stream<SummaryWindow> getSummaryWindowsOverlapping(StreamWindowManager windowManager, long t0, long t1)
            throws BackingStoreException {
        try {
            Iterator<SummaryWindow> iterator = new OverlappingSWIterator(windowManager, t0, t1);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
        } catch (RocksDBException e) {
            throw new BackingStoreException(e);
        }
    }

    @Override
    long getNumSummaryWindows(StreamWindowManager windowManager) {
        RocksIterator iter = null;
        try {
            iter = rocksDB.newIterator();
            iter.seek(getRocksDBKey(windowManager.streamID, 0L));
            long ct = 0;
            while (iter.isValid() && parseRocksDBKeyStreamID(iter.key()) == windowManager.streamID) {
                ++ct;
                iter.next();
            }
            return ct;
        } finally {
            if (iter != null) iter.dispose();
        }
    }

    @Override
    void putSummaryWindow(StreamWindowManager windowManager, long swid, SummaryWindow window) throws BackingStoreException {
        if (cache != null) {
            ConcurrentHashMap<Long, SummaryWindow> streamCache = cache.get(windowManager.streamID);
            if (streamCache == null) cache.put(windowManager.streamID, streamCache = new ConcurrentHashMap<>());

            try {
                insertIntoCache(streamCache, windowManager, swid, window);
            } catch (RocksDBException e) {
                throw new BackingStoreException(e);
            }
        } else {
            byte[] key = getRocksDBKey(windowManager.streamID, swid);
            byte[] value = windowManager.serializeSummaryWindow(window);
            try {
                rocksDB.put(key, value);
            } catch (RocksDBException e) {
                throw new BackingStoreException(e);
            }
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

    /* **** <FIXME> Landmark cache has unbounded size **** */

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

    /* **** </FIXME>  **** */

    @Override
    void printWindowState(StreamWindowManager windowManager) throws BackingStoreException {
        throw new UnsupportedOperationException("RocksDB backing store does not yet implement printWindowState");
    }

    @Override
    public void close() throws BackingStoreException {
        if (rocksDB != null) {
            rocksDB.close();
        }
        rocksDBOptions.dispose();
        logger.info("rocksDB closed");
    }
}
