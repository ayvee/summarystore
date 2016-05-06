package com.samsung.sra.DataStore;

import org.nustaq.serialization.FSTConfiguration;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDBBucketStore implements BucketStore {
    private RocksDB rocksDB = null;
    private Options rocksDBOptions = null;

    public RocksDBBucketStore(String rocksPath) throws RocksDBException {
        rocksDBOptions = new Options().setCreateIfMissing(true);
        rocksDB = RocksDB.open(rocksDBOptions, rocksPath);
    }

    // FST is a fast serialization library, used to quickly convert Buckets to/from RocksDB byte arrays
    private static final FSTConfiguration fstConf;

    static {
        fstConf = FSTConfiguration.createDefaultConfiguration();
        fstConf.registerClass(Bucket.class);
        fstConf.registerClass(SummaryStore.StreamInfo.class);
        fstConf.registerClass(CountBasedWBMH.class);

        RocksDB.loadLibrary();
    }

    byte[] keyArray = new byte[16];
    byte[] valueArray = new byte[72];

    /** stuff val into array[startPos], array[startPos+1], ..., array[startPos+7] */
    void longToByteArray(long val, byte[] array, int startPos) {
        for (int i = startPos + 7; i >= startPos; --i) {
            array[i] = (byte)(val & 0xFF);
            val >>= 8;
        }
    }

    /** return the long represented by array[startPos], array[startPos+1], ..., array[startPos+7] */
    long byteArrayToLong(byte[] array, int startPos) {
        long val = 0;
        for (int i = startPos; i < startPos + 8; ++i) {
            val <<= 8;
            val |= (array[i] & 0xFF);
        }
        return val;
    }

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

    @Override
    public Bucket getBucket(long streamID, long bucketID, boolean delete) throws RocksDBException {
        byte[] rocksKey = getRocksDBKey(streamID, bucketID);
        byte[] rocksValue = rocksDB.get(rocksKey);
        if (delete) {
            rocksDB.remove(rocksKey);
        }
        return byteArrayToBucket(rocksValue);
    }

    @Override
    public void putBucket(long streamID, long bucketID, Bucket bucket) throws RocksDBException {
        byte[] rocksKey = getRocksDBKey(streamID, bucketID);
        byte[] rocksValue = bucketToByteArray(bucket);
        rocksDB.put(rocksKey, rocksValue);
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
            rocksDB.close();
        }
        rocksDBOptions.dispose();
    }
}
