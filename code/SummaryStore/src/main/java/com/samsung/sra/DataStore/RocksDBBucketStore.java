package com.samsung.sra.DataStore;

import org.apache.commons.lang.SerializationUtils;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RocksDBBucketStore implements BucketStore {
    private final RocksDB rocksDB;
    private final Options rocksDBOptions;
    private final long cacheSizePerStream;
    // TODO: use a LinkedHashMap in LRU mode
    private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, Bucket>> cache; // map streamID -> bucketID -> bucket

    /**
     * @param rocksPath  on-disk path
     * @param cacheSizePerStream  number of elements per stream to cache in main memory. Set to 0 to disable caching
     * @throws RocksDBException
     */
    public RocksDBBucketStore(String rocksPath, long cacheSizePerStream) throws RocksDBException {
        this.cacheSizePerStream = cacheSizePerStream;
        cache = cacheSizePerStream > 0 ? new ConcurrentHashMap<>() : null;
        rocksDBOptions = new Options().setCreateIfMissing(true);
        rocksDB = RocksDB.open(rocksDBOptions, rocksPath);
    }

    static {
        RocksDB.loadLibrary();
    }

    private static final int KEY_SIZE = 16;

    /**
     * RocksDB key = <streamID, bucketID>. Since we ensure bucketIDs are assigned in increasing
     * order, this lays out data in temporal order within streams
     */
    private byte[] getRocksDBKey(long streamID, long bucketID) {
        byte[] keyArray = new byte[KEY_SIZE];
        Utilities.longToByteArray(streamID, keyArray, 0);
        Utilities.longToByteArray(bucketID, keyArray, 8);
        return keyArray;
    }

    private long parseRocksDBKeyStreamID(byte[] keyArray) {
        return Utilities.byteArrayToLong(keyArray, 0);
    }

    private long parseRocksDBKeyBucketID(byte[] keyArray) {
        return Utilities.byteArrayToLong(keyArray, 8);
    }

    /**
     * Insert value into cache, evicting another cached entry if necessary
     */
    private void insertIntoCache(Map<Long, Bucket> streamCache, StreamManager streamManager, long bucketID, Bucket bucket) throws RocksDBException {
        assert streamCache != null;
        if (streamCache.size() >= cacheSizePerStream) { // evict something
            Map.Entry<Long, Bucket> evicted = streamCache.entrySet().iterator().next();
            byte[] evictedKey = getRocksDBKey(streamManager.streamID, evicted.getKey());
            byte[] evictedValue = streamManager.serializeBucket(evicted.getValue());
            rocksDB.put(evictedKey, evictedValue);
        }
        streamCache.put(bucketID, bucket);
    }

    @Override
    public Bucket getBucket(StreamManager streamManager, long bucketID, boolean delete) throws RocksDBException {
        ConcurrentHashMap<Long, Bucket> streamCache;
        if (cache == null) {
            streamCache = null;
        } else {
            streamCache = cache.get(streamManager.streamID);
            if (streamCache == null) cache.put(streamManager.streamID, streamCache = new ConcurrentHashMap<>());
        }

        Bucket bucket = streamCache != null ? streamCache.get(bucketID) : null;
        if (bucket != null) { // cache hit
            if (delete) streamCache.remove(bucketID);
            return bucket;
        } else { // either no cache or cache miss; read-through from RocksDB
            byte[] rocksKey = getRocksDBKey(streamManager.streamID, bucketID);
            byte[] rocksValue = rocksDB.get(rocksKey);
            bucket = streamManager.deserializeBucket(rocksValue);
            if (delete) {
                rocksDB.remove(rocksKey);
            }
            if (streamCache != null) {
                insertIntoCache(streamCache, streamManager, bucketID, bucket);
            }
            return bucket;
        }
    }

    @Override
    public void putBucket(StreamManager streamManager, long bucketID, Bucket bucket) throws RocksDBException {
        if (cache != null) {
            ConcurrentHashMap<Long, Bucket> streamCache = cache.get(streamManager.streamID);
            if (streamCache == null) cache.put(streamManager.streamID, streamCache = new ConcurrentHashMap<>());

            insertIntoCache(streamCache, streamManager, bucketID, bucket);
        } else {
            byte[] key = getRocksDBKey(streamManager.streamID, bucketID);
            byte[] value = streamManager.serializeBucket(bucket);
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
                ConcurrentHashMap<Long, Bucket> streamCache = cache.get(streamID);
                if (streamCache == null) {
                    cache.put(streamID, streamCache = new ConcurrentHashMap<>());
                } else if (streamCache.size() >= cacheSizePerStream) {
                    continue;
                }
                long bucketID = parseRocksDBKeyBucketID(keyArray);
                Bucket bucket = streamManager.deserializeBucket(iter.value());
                streamCache.put(bucketID, bucket);
            }
        } finally {
            if (iter != null) iter.dispose();
        }
    }

    @Override
    public void flushCache(StreamManager streamManager) throws RocksDBException {
        if (cache == null) return;
        Map<Long, Bucket> streamCache = cache.get(streamManager.streamID);
        if (streamCache != null) {
            for (Map.Entry<Long, Bucket> bucketEntry : streamCache.entrySet()) {
                long bucketID = bucketEntry.getKey();
                Bucket bucket = bucketEntry.getValue();
                byte[] rocksKey = getRocksDBKey(streamManager.streamID, bucketID);
                byte[] rocksValue = streamManager.serializeBucket(bucket);
                rocksDB.put(rocksKey, rocksValue);
            }
        }
    }

    /** We will persist metadata in RocksDB under this special (empty) key, which will
     * never collide with any of the (non-empty) keys we use for bucket storage
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
        if (rocksDB != null) {
            rocksDB.close();
        }
        rocksDBOptions.dispose();
    }
}
