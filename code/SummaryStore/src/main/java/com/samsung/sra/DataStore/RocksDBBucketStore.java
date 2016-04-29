package com.samsung.sra.DataStore;

import org.nustaq.serialization.FSTConfiguration;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;

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
    private byte[] getRocksDBKey(StreamID streamID, BucketID bucketID) {
        ByteBuffer bytebuf = ByteBuffer.allocate(StreamID.byteCount + BucketID.byteCount);
        streamID.writeToByteBuffer(bytebuf);
        bucketID.writeToByteBuffer(bytebuf);
        bytebuf.flip();
        return bytebuf.array();
    }

    @Override
    public Bucket getBucket(StreamID streamID, BucketID bucketID, boolean delete) throws RocksDBException {
        byte[] rocksKey = getRocksDBKey(streamID, bucketID);
        byte[] rocksValue = rocksDB.get(rocksKey);
        if (delete) {
            rocksDB.remove(rocksKey);
        }
        return (Bucket)fstConf.asObject(rocksValue);
    }

    @Override
    public void putBucket(StreamID streamID, BucketID bucketID, Bucket bucket) throws RocksDBException {
        byte[] rocksKey = getRocksDBKey(streamID, bucketID);
        byte[] rocksValue = fstConf.asByteArray(bucket);
        rocksDB.put(rocksKey, rocksValue);
    }

    /** We will persist indexes in RocksDB under this special key. Note that this key is 1 byte, as
     * opposed to the 8 byte keys we use for buckets, so it won't interfere with bucket storage
     */
    private final static byte[] indexesSpecialKey = {0};

    @Override
    public Object getIndexes() throws RocksDBException {
        byte[] indexesBytes = rocksDB.get(indexesSpecialKey);
        return indexesBytes != null ?
                fstConf.asObject(indexesBytes) :
                null;
    }

    @Override
    public void putIndexes(Object indexes) throws RocksDBException {
        rocksDB.put(indexesSpecialKey, fstConf.asByteArray(indexes));
    }

    @Override
    public void close() throws RocksDBException {
        if (rocksDB != null) {
            rocksDB.close();
        }
        rocksDBOptions.dispose();
    }
}
