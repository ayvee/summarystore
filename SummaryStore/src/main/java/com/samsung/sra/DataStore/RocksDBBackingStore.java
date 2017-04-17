package com.samsung.sra.DataStore;

import org.apache.commons.lang.SerializationUtils;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RocksDBBackingStore implements BackingStore {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBBackingStore.class);

    private final RocksDB rocksDB;
    private final Options rocksDBOptions;
    private final long cacheSizePerStream;
    private final String landmarksFile;
    // TODO: use a LinkedHashMap in LRU mode
    // map streamID -> windowID -> window
    private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, SummaryWindow>> cache;

    /**
     * @param rocksPath  on-disk path
     * @param cacheSizePerStream  number of elements per stream to cache in main memory. Set to 0 to disable caching
     * @throws RocksDBException
     */
    public RocksDBBackingStore(String rocksPath, long cacheSizePerStream, String landmarksFile) throws RocksDBException {
        this.cacheSizePerStream = cacheSizePerStream;
        cache = cacheSizePerStream > 0 ? new ConcurrentHashMap<>() : null;
        rocksDBOptions = new Options().setCreateIfMissing(true);
        rocksDB = RocksDB.open(rocksDBOptions, rocksPath);
        this.landmarksFile = landmarksFile;
        deserializeLandmarks();
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
    private void insertIntoCache(ConcurrentHashMap<Long, SummaryWindow> streamCache, StreamManager streamManager,
                                 long swid, SummaryWindow window) throws RocksDBException {
        assert streamCache != null;
        if (streamCache.size() >= cacheSizePerStream) { // evict something
            Map.Entry<Long, SummaryWindow> evicted = streamCache.entrySet().iterator().next();
            byte[] evictedKey = getRocksDBKey(streamManager.streamID, evicted.getKey());
            byte[] evictedValue = streamManager.serializeSummaryWindow(evicted.getValue());
            rocksDB.put(evictedKey, evictedValue);
        }
        streamCache.put(swid, window);
    }

    private SummaryWindow getAndOrDeleteSummaryWindow(StreamManager streamManager, long swid, boolean delete) throws RocksDBException {
        ConcurrentHashMap<Long, SummaryWindow> streamCache;
        if (cache == null) {
            streamCache = null;
        } else {
            streamCache = cache.get(streamManager.streamID);
            if (streamCache == null) cache.put(streamManager.streamID, streamCache = new ConcurrentHashMap<>());
        }

        SummaryWindow window = streamCache != null ? streamCache.get(swid) : null;
        if (window != null) { // cache hit
            if (delete) streamCache.remove(swid);
            return window;
        } else { // either no cache or cache miss; read-through from RocksDB
            byte[] rocksKey = getRocksDBKey(streamManager.streamID, swid);
            byte[] rocksValue = rocksDB.get(rocksKey);
            window = streamManager.deserializeSummaryWindow(rocksValue);
            if (delete) {
                rocksDB.remove(rocksKey);
            }
            if (streamCache != null) {
                insertIntoCache(streamCache, streamManager, swid, window);
            }
            return window;
        }
    }

    @Override
    public SummaryWindow getSummaryWindow(StreamManager streamManager, long swid) throws RocksDBException {
        return getAndOrDeleteSummaryWindow(streamManager, swid, false);
    }

    @Override
    public SummaryWindow deleteSummaryWindow(StreamManager streamManager, long swid) throws RocksDBException {
        return getAndOrDeleteSummaryWindow(streamManager, swid, true);
    }

    @Override
    public void putSummaryWindow(StreamManager streamManager, long swid, SummaryWindow window) throws RocksDBException {
        if (cache != null) {
            ConcurrentHashMap<Long, SummaryWindow> streamCache = cache.get(streamManager.streamID);
            if (streamCache == null) cache.put(streamManager.streamID, streamCache = new ConcurrentHashMap<>());

            insertIntoCache(streamCache, streamManager, swid, window);
        } else {
            byte[] key = getRocksDBKey(streamManager.streamID, swid);
            byte[] value = streamManager.serializeSummaryWindow(window);
            rocksDB.put(key, value);
        }
    }

    @Override
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
    }

    @Override
    public void flushCache(StreamManager streamManager) throws RocksDBException {
        if (cache == null) return;
        Map<Long, SummaryWindow> streamCache = cache.get(streamManager.streamID);
        if (streamCache != null) {
            for (Map.Entry<Long, SummaryWindow> entry: streamCache.entrySet()) {
                long swid = entry.getKey();
                SummaryWindow window = entry.getValue();
                byte[] rocksKey = getRocksDBKey(streamManager.streamID, swid);
                byte[] rocksValue = streamManager.serializeSummaryWindow(window);
                rocksDB.put(rocksKey, rocksValue);
            }
        }
    }

    /* **** <FIXME> Holding all landmarks in main memory for now (only persisting to disk on close)  **** */

    private ConcurrentHashMap<Long, ConcurrentHashMap<Long, LandmarkWindow>> landmarkWindows;
    //private static final byte[] landmarksSpecialKey = {'l'}; // 1-byte special key, does not collide with any of the 16-byte summary windows

    private void deserializeLandmarks() throws RocksDBException {
        File file;
        if (landmarksFile == null || !(file = new File(landmarksFile)).exists()) {
            landmarkWindows = new ConcurrentHashMap<>();
        } else {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
                landmarkWindows = (ConcurrentHashMap) ois.readObject();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        /*byte[] landmarksSer = rocksDB.get(landmarksSpecialKey);
        landmarkWindows = landmarksSer != null
                ? (ConcurrentHashMap<Long, ConcurrentHashMap<Long, LandmarkWindow>>) SerializationUtils.deserialize(landmarksSer)
                : new ConcurrentHashMap<>();*/
    }

    private void serializeLandmarks() throws RocksDBException {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(landmarksFile))) {
            oos.writeObject(landmarkWindows);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        /*byte[] landmarksSer = SerializationUtils.serialize(landmarkWindows);
        rocksDB.put(landmarksSpecialKey, landmarksSer);*/
    }

    @Override
    public LandmarkWindow getLandmarkWindow(StreamManager streamManager, long lwid) throws RocksDBException {
        return landmarkWindows.get(streamManager.streamID).get(lwid);
    }

    @Override
    public void putLandmarkWindow(StreamManager streamManager, long lwid, LandmarkWindow window) throws RocksDBException {
        ConcurrentHashMap<Long, LandmarkWindow> stream = landmarkWindows.get(streamManager.streamID);
        if (stream == null) {
            landmarkWindows.put(streamManager.streamID, (stream = new ConcurrentHashMap<>()));
        }
        stream.put(lwid, window);
    }

    /* **** </FIXME>  **** */

    /** We will persist metadata in RocksDB under this special (empty) key, which will
     * never collide with any of the (non-empty) keys we use for window storage
     */
    private final static byte[] metadataSpecialKey = {};

    // FIXME: NA; also use proto here
    @Override
    public Serializable getMetadata() throws RocksDBException {
        byte[] indexesBytes = rocksDB.get(metadataSpecialKey);
        return indexesBytes != null ?
                (Serializable)SerializationUtils.deserialize(indexesBytes) :
                null;
    }

    @Override
    public void putMetadata(Serializable indexes) throws RocksDBException {
        rocksDB.put(metadataSpecialKey, SerializationUtils.serialize(indexes));
    }

    @Override
    public void close() throws RocksDBException {
        serializeLandmarks();
        if (rocksDB != null) {
            rocksDB.close();
        }
        rocksDBOptions.dispose();
        logger.info("rocksDB closed; should be safe to terminate this process now");
    }
}
