package com.samsung.sra.DataStore.Storage;

import com.samsung.sra.DataStore.LandmarkWindow;
import com.samsung.sra.DataStore.SummaryWindow;
import com.samsung.sra.DataStore.Utilities;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RocksDBBackingStore extends BackingStore {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBBackingStore.class);

    private final RocksDB rocksDB;
    private final Options rocksDBOptions;
    private final WriteOptions rocksDBWriteOptions;
    private final long cacheSizePerStream;
    /** Map streamID -> windowID -> window */
    private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, SummaryWindow>> cache;

    /**
     * @param rocksPath  on-disk path
     * @param cacheSizePerStream  number of elements per stream to cache in main memory. Set to 0 to disable caching.
     *                            Should only be used in readonly mode
     * @throws BackingStoreException  wrapping RocksDBException
     */
    public RocksDBBackingStore(String rocksPath, long cacheSizePerStream) throws BackingStoreException {
        this.cacheSizePerStream = cacheSizePerStream;
        cache = cacheSizePerStream > 0 ? new ConcurrentHashMap<>() : null;
        rocksDBOptions = new Options()
                .setCreateIfMissing(true)
                .setDbLogDir("/tmp")
                .createStatistics()
                .setStatsDumpPeriodSec(300)
                .setMaxBackgroundCompactions(2)
                .setMaxBackgroundFlushes(2)
                .setDbWriteBufferSize(512L * 1024 * 1024)
                .setMaxWriteBufferNumber(3)
                .setMinWriteBufferNumberToMerge(1)
                .setLevel0FileNumCompactionTrigger(4)
                .setLevel0SlowdownWritesTrigger(10_000)
                .setLevel0StopWritesTrigger(10_000)
                .setSoftPendingCompactionBytesLimit(1_000_000_000_000_000L)
                .setHardPendingCompactionBytesLimit(1_000_000_000_000_000L)
                .setMaxBytesForLevelBase(512L * 1024 * 1024 * 1 * 4)
                .setTargetFileSizeBase(512L * 1024 * 1024)
                .setLevelCompactionDynamicLevelBytes(true)
                .setCompactionReadaheadSize(10L * 1024 * 1024)
                .setNewTableReaderForCompactionInputs(true)
                //.setCompactionStyle(CompactionStyle.UNIVERSAL)
                .setMemTableConfig(new VectorMemTableConfig())
                .setAllowConcurrentMemtableWrite(false)
                .setTableFormatConfig(new BlockBasedTableConfig()
                        .setBlockSize(256L * 1024)
                        .setBlockCacheSize(5L * 1024 * 1024 * 1024)
                        .setCacheIndexAndFilterBlocks(true)
                        //.setFilter(new BloomFilter())
                )
                //.setOptimizeFiltersForHits(true)
                .setMaxOpenFiles(-1);
        /*rocksDBOptions = new Options()
                .setCreateIfMissing(true)
                .setDbLogDir("/tmp")
                .createStatistics()
                .setStatsDumpPeriodSec(300)
                .setMaxBackgroundCompactions(10)
                .setMaxBackgroundFlushes(10)
                .setAllowConcurrentMemtableWrite(true)
                .setDbWriteBufferSize(512L * 1024 * 1024)
                .setMaxWriteBufferNumber(24)
                .setMinWriteBufferNumberToMerge(4)
                .setLevel0FileNumCompactionTrigger(4)
                .setLevel0SlowdownWritesTrigger(10_000)
                .setLevel0StopWritesTrigger(10_000)
                .setSoftPendingCompactionBytesLimit(1_000_000_000_000_000L)
                .setHardPendingCompactionBytesLimit(1_000_000_000_000_000L)
                .setMaxBytesForLevelBase(512L * 1024 * 1024 * 4 * 4)
                .setTargetFileSizeBase(512L * 1024 * 1024)
                .setLevelCompactionDynamicLevelBytes(true)
                .setCompactionReadaheadSize(10L * 1024 * 1024)
                .setNewTableReaderForCompactionInputs(true)
                //.setCompactionStyle(CompactionStyle.UNIVERSAL)
                .setTableFormatConfig(new BlockBasedTableConfig()
                                .setBlockSize(256L * 1024)
                                .setBlockCacheSize(50L * 1024 * 1024 * 1024)
                        setCacheIndexAndFilterBlocks(true)
                        //.setFilter(new BloomFilter())
                )
                //.setOptimizeFiltersForHits(true)
                .setMaxOpenFiles(-1);*/
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

    private void insertIntoCache(ConcurrentHashMap<Long, SummaryWindow> streamCache, long swid, SummaryWindow window) {
        if (streamCache.size() >= cacheSizePerStream) { // evict random
            Map.Entry<Long, SummaryWindow> evictedEntry = streamCache.entrySet().iterator().next(); // basically a random evict
            streamCache.remove(evictedEntry.getKey());
        }
        streamCache.put(swid, window);
    }

    @Override
    SummaryWindow getSummaryWindow(long streamID, long swid, SerDe serDe) throws BackingStoreException {
        ConcurrentHashMap<Long, SummaryWindow> streamCache;
        if (cache == null) {
            streamCache = null;
        } else {
            streamCache = cache.get(streamID);
            if (streamCache == null) cache.put(streamID, streamCache = new ConcurrentHashMap<>());
        }

        SummaryWindow window = streamCache != null ? streamCache.get(swid) : null;
        if (window != null) { // cache hit
            return window;
        } else { // either no cache or cache miss; read-through from RocksDB
            byte[] rocksKey = getRocksDBKey(streamID, swid);
            try {
                byte[] rocksValue = rocksDB.get(rocksKey);
                window = serDe.deserializeSummaryWindow(rocksValue);
            } catch (RocksDBException e) {
                throw new BackingStoreException(e);
            }
            if (streamCache != null) insertIntoCache(streamCache, swid, window);
            return window;
        }
    }

    @Override
    void deleteSummaryWindow(long streamID, long swid, SerDe serDe) throws BackingStoreException {
        assert cache == null;
        try {
            byte[] key = getRocksDBKey(streamID, swid);
            rocksDB.delete(key);
        } catch (RocksDBException e) {
            throw new BackingStoreException(e);
        }
    }

    @Override
    void putSummaryWindow(long streamID, long swid, SerDe serDe, SummaryWindow window) throws BackingStoreException {
        assert cache == null;
        try {
            byte[] key = getRocksDBKey(streamID, swid);
            byte[] value = serDe.serializeSummaryWindow(window);
            rocksDB.put(rocksDBWriteOptions, key, value);
        } catch (RocksDBException e) {
            throw new BackingStoreException(e);
        }
    }

    @Override
    public void flushToDisk(long streamID, SerDe serDe) throws BackingStoreException {
        flushLandmarksToDisk(streamID, serDe);
        if (cache == null) return;
        Map<Long, SummaryWindow> streamCache = cache.get(streamID);
        if (streamCache != null) {
            for (Map.Entry<Long, SummaryWindow> entry: streamCache.entrySet()) {
                long swid = entry.getKey();
                SummaryWindow window = entry.getValue();
                byte[] rocksKey = getRocksDBKey(streamID, swid);
                byte[] rocksValue = serDe.serializeSummaryWindow(window);
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

    private void flushLandmarksToDisk(long streamID, SerDe serDe) throws BackingStoreException {
        Map<Long, LandmarkWindow> streamMap = landmarkCache.get(streamID);
        if (streamMap == null) return;
        for (Map.Entry<Long, LandmarkWindow> windowEntry: streamMap.entrySet()) {
            long lwid = windowEntry.getKey();
            LandmarkWindow window = windowEntry.getValue();
            try {
                rocksDB.put(getLandmarkRocksKey(streamID, lwid), serDe.serializeLandmarkWindow(window));
            } catch (RocksDBException e) {
                throw new BackingStoreException(e);
            }
        }
    }

    @Override
    LandmarkWindow getLandmarkWindow(long streamID, long lwid, SerDe serDe) throws BackingStoreException {
        Map<Long, LandmarkWindow> streamMap = landmarkCache.get(streamID);
        if (streamMap != null && streamMap.containsKey(lwid)) {
            return streamMap.get(lwid);
        } else {
            byte[] bytes;
            try {
                bytes = rocksDB.get(getLandmarkRocksKey(streamID, lwid));
            } catch (RocksDBException e) {
                throw new BackingStoreException(e);
            }
            return serDe.deserializeLandmarkWindow(bytes);
        }
    }

    @Override
    void putLandmarkWindow(long streamID, long lwid, SerDe serDe, LandmarkWindow window) {
        ConcurrentHashMap<Long, LandmarkWindow> stream = landmarkCache.get(streamID);
        if (stream == null) landmarkCache.put(streamID, (stream = new ConcurrentHashMap<>()));
        stream.put(lwid, window);
    }

    @Override
    public void close() throws BackingStoreException {
        if (rocksDB != null) rocksDB.close();
        rocksDBOptions.close();
        logger.info("rocksDB closed");
    }
}
