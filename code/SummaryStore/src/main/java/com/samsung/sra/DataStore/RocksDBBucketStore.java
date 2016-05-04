package com.samsung.sra.DataStore;

import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.FSTObjectOutput;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;

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

    /**
     * RocksDB key = <streamID, bucketID>. Since we ensure bucketIDs are assigned in increasing
     * order, this lays out data in temporal order within streams
     */
    private byte[] getRocksDBKey(long streamID, long bucketID) {
        FSTObjectOutput out = fstConf.getObjectOutput();
        try {
            out.writeLong(streamID);
            out.writeLong(bucketID);
        } catch (IOException e) {
            throw new RuntimeException("FST failed to serialize long", e);
        }
        return out.getCopyOfWrittenBuffer();
    }

    @Override
    public Bucket getBucket(long streamID, long bucketID, boolean delete) throws RocksDBException {
        byte[] rocksKey = getRocksDBKey(streamID, bucketID);
        byte[] rocksValue = rocksDB.get(rocksKey);
        if (delete) {
            rocksDB.remove(rocksKey);
        }
        return (Bucket)fstConf.asObject(rocksValue);
    }

    @Override
    public void putBucket(long streamID, long bucketID, Bucket bucket) throws RocksDBException {
        byte[] rocksKey = getRocksDBKey(streamID, bucketID);
        byte[] rocksValue = fstConf.asByteArray(bucket);
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
