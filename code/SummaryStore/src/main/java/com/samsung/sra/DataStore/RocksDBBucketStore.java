package com.samsung.sra.DataStore;

import org.nustaq.serialization.FSTConfiguration;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.HashMap;
import java.util.Map;

public class RocksDBBucketStore implements BucketStore {
    private final RocksDB rocksDB;
    private final Options rocksDBOptions;
    private final long cacheSizePerStream;
    private final Map<Long, Map<Long, Bucket>> cache; // map streamID -> bucketID -> bucket

    /**
     * @param rocksPath  on-disk path
     * @param cacheSizePerStream  number of elements per stream to cache in main memory. Set to 0 to disable caching
     * @throws RocksDBException
     */
    public RocksDBBucketStore(String rocksPath, long cacheSizePerStream) throws RocksDBException {
        this.cacheSizePerStream = cacheSizePerStream;
        cache = cacheSizePerStream > 0 ? new HashMap<>() : null;
        rocksDBOptions = new Options().setCreateIfMissing(true);
        rocksDB = RocksDB.open(rocksDBOptions, rocksPath);
    }

    public RocksDBBucketStore(String rocksPath) throws RocksDBException {
        this(rocksPath, 0);
    }

    // FST is a fast serialization library, originally used to convert Buckets
    // to/from RocksDB byte arrays, now only used to serialize metadata
    private static final FSTConfiguration fstConf;

    static {
        fstConf = FSTConfiguration.createDefaultConfiguration();
        //fstConf.registerClass(Bucket.class);
        fstConf.registerClass(SummaryStore.StreamInfo.class);
        fstConf.registerClass(CountBasedWBMH.class);

        RocksDB.loadLibrary();
    }

    /** stuff val into array[startPos], array[startPos+1], ..., array[startPos+7] */
    void longToByteArray(long val, byte[] array, int startPos) {
        array[startPos    ] = (byte) ((val >> 56) & 0xFFL);
        array[startPos + 1] = (byte) ((val >> 48) & 0xFFL);
        array[startPos + 2] = (byte) ((val >> 40) & 0xFFL);
        array[startPos + 3] = (byte) ((val >> 32) & 0xFFL);
        array[startPos + 4] = (byte) ((val >> 24) & 0xFFL);
        array[startPos + 5] = (byte) ((val >> 16) & 0xFFL);
        array[startPos + 6] = (byte) ((val >> 8)  & 0xFFL);
        array[startPos + 7] = (byte)  (val        & 0xFFL);
    }

    /** return the long represented by array[startPos], array[startPos+1], ..., array[startPos+7] */
    long byteArrayToLong(byte[] array, int startPos) {
        return
                (((long) array[startPos    ] & 0xFFL) << 56) |
                (((long) array[startPos + 1] & 0xFFL) << 48) |
                (((long) array[startPos + 2] & 0xFFL) << 40) |
                (((long) array[startPos + 3] & 0xFFL) << 32) |
                (((long) array[startPos + 4] & 0xFFL) << 24) |
                (((long) array[startPos + 5] & 0xFFL) << 16) |
                (((long) array[startPos + 6] & 0xFFL) << 8) |
                ((long)  array[startPos + 7] & 0xFFL);
    }

    // FIXME: we will use a single pre-allocated byte array for every RocksDB key and value, instead
    // of creating new arrays on each operation. Reusing byte arrays avoids GC overhead, but only works
    // in single-threaded mode, and will break if we append to multiple streams in parallel.
    byte[] keyArray = new byte[16];
    byte[] valueArray = new byte[72];

    /**
     * RocksDB key = <streamID, bucketID>. Since we ensure bucketIDs are assigned in increasing
     * order, this lays out data in temporal order within streams
     */
    private byte[] getRocksDBKey(long streamID, long bucketID) {
        longToByteArray(streamID, keyArray, 0);
        longToByteArray(bucketID, keyArray, 8);
        return keyArray;
    }

    private byte[] bucketToByteArray(Bucket bucket) {
        longToByteArray(bucket.count, valueArray, 0);
        longToByteArray(bucket.sum, valueArray, 8);
        longToByteArray(bucket.prevBucketID, valueArray, 16);
        longToByteArray(bucket.thisBucketID, valueArray, 24);
        longToByteArray(bucket.nextBucketID, valueArray, 32);
        longToByteArray(bucket.tStart, valueArray, 40);
        longToByteArray(bucket.tEnd, valueArray, 48);
        longToByteArray(bucket.cStart, valueArray, 56);
        longToByteArray(bucket.cEnd, valueArray, 64);
        return valueArray;
    }

    private Bucket byteArrayToBucket(byte[] array) {
        assert array.length == 72;
        long count = byteArrayToLong(array, 0);
        long sum = byteArrayToLong(array, 8);
        long prevBucketID = byteArrayToLong(array, 16);
        long thisBucketID = byteArrayToLong(array, 24);
        long nextBucketID = byteArrayToLong(array, 32);
        long tStart = byteArrayToLong(array, 40);
        long tEnd = byteArrayToLong(array, 48);
        long cStart = byteArrayToLong(array, 56);
        long cEnd = byteArrayToLong(array, 64);
        return new Bucket(count, sum, prevBucketID, thisBucketID, nextBucketID, tStart, tEnd, cStart, cEnd);
    }

    /**
     * Insert value into cache, evicting another cached entry if necessary
     */
    private void insertIntoCache(Map<Long, Bucket> streamCache, long streamID, long bucketID, Bucket bucket) throws RocksDBException {
        assert streamCache != null;
        if (streamCache.size() >= cacheSizePerStream) { // evict something
            Map.Entry<Long, Bucket> evicted = streamCache.entrySet().iterator().next();
            byte[] evictedKey = getRocksDBKey(streamID, evicted.getKey());
            byte[] evictedValue = bucketToByteArray(evicted.getValue());
            rocksDB.put(evictedKey, evictedValue);
        }
        streamCache.put(bucketID, bucket);
    }

    @Override
    public Bucket getBucket(long streamID, long bucketID, boolean delete) throws RocksDBException {
        Map<Long, Bucket> streamCache;
        if (cache == null) {
            streamCache = null;
        } else {
            streamCache = cache.get(streamID);
            if (streamCache == null) cache.put(streamID, streamCache = new HashMap<>());
        }

        Bucket bucket = streamCache != null ? streamCache.get(bucketID) : null;
        if (bucket != null) { // cache hit
            if (delete) streamCache.remove(bucketID);
            return bucket;
        } else { // either no cache or cache miss; read-through from RocksDB
            byte[] rocksKey = getRocksDBKey(streamID, bucketID);
            rocksDB.get(rocksKey, valueArray);
            bucket = byteArrayToBucket(valueArray);
            if (delete) {
                rocksDB.remove(rocksKey);
            }
            if (streamCache != null) {
                insertIntoCache(streamCache, streamID, bucketID, bucket);
            }
            return bucket;
        }
    }

    @Override
    public void putBucket(long streamID, long bucketID, Bucket bucket) throws RocksDBException {
        if (cache != null) {
            Map<Long, Bucket> streamCache = cache.get(streamID);
            if (streamCache == null) cache.put(streamID, streamCache = new HashMap<>());

            insertIntoCache(streamCache, streamID, bucketID, bucket);
        } else {
            byte[] key = getRocksDBKey(streamID, bucketID);
            byte[] value = bucketToByteArray(bucket);
            rocksDB.put(key, value);
        }
    }

    /** We will persist metadata in RocksDB under this special (empty) key, which will
     * never collide with any of the (non-empty) keys we use for bucket storage
     */
    private final static byte[] metadataSpecialKey = {};

    @Override
    public Object getMetadata() throws RocksDBException {
        byte[] indexesBytes = rocksDB.get(metadataSpecialKey);
        return indexesBytes != null ?
                fstConf.asObject(indexesBytes) :
                null;
    }

    @Override
    public void putMetadata(Object indexes) throws RocksDBException {
        rocksDB.put(metadataSpecialKey, fstConf.asByteArray(indexes));
    }

    @Override
    public void close() throws RocksDBException {
        if (rocksDB != null) {
            if (cache != null) {
                // flush cache to disk
                for (Map.Entry<Long, Map<Long, Bucket>> streamEntry : cache.entrySet()) {
                    long streamID = streamEntry.getKey();
                    for (Map.Entry<Long, Bucket> bucketEntry : streamEntry.getValue().entrySet()) {
                        long bucketID = bucketEntry.getKey();
                        Bucket bucket = bucketEntry.getValue();
                        byte[] rocksKey = getRocksDBKey(streamID, bucketID);
                        byte[] rocksValue = bucketToByteArray(bucket);
                        rocksDB.put(rocksKey, rocksValue);
                    }
                }
            }
            rocksDB.close();
        }
        rocksDBOptions.dispose();
    }
}
