package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;

import java.util.HashMap;
import java.util.Map;

public class MainMemoryBucketStore implements BucketStore {
    private Map<StreamID, Map<BucketID, Bucket>> buckets = new HashMap<>();

    @Override
    public Bucket getBucket(StreamID streamID, BucketID bucketID, boolean delete) throws RocksDBException {
        return delete ?
                buckets.get(streamID).remove(bucketID) :
                buckets.get(streamID).get(bucketID);
    }

    @Override
    public void putBucket(StreamID streamID, BucketID bucketID, Bucket bucket) throws RocksDBException {
        Map<BucketID, Bucket> stream = buckets.get(streamID);
        if (stream == null) {
            buckets.put(streamID, (stream = new HashMap<>()));
        }
        stream.put(bucketID, bucket);
    }

    private Object indexes = null;

    @Override
    public Object getIndexes() throws RocksDBException {
        return indexes;
    }

    @Override
    public void putIndexes(Object indexes) throws RocksDBException {
        this.indexes = indexes;
    }

    @Override
    public void close() throws RocksDBException {
        buckets.clear();
        indexes = null;
    }
}
