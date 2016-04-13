package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;

/**
 * Underlying key-value store holding all the buckets. Two implementations:
 *      RocksDBBucketStore
 *      MainMemoryBucketStore
 */
interface BucketStore extends AutoCloseable {
    Bucket getBucket(StreamID streamID, BucketID bucketID, boolean delete) throws RocksDBException;

    default Bucket getBucket(StreamID streamID, BucketID bucketID) throws RocksDBException {
        return getBucket(streamID, bucketID, false);
    }

    void putBucket(StreamID streamID, BucketID bucketID, Bucket bucket) throws RocksDBException;

    Object getIndexes() throws RocksDBException;

    void putIndexes(Object indexes) throws RocksDBException;

    @Override
    void close() throws RocksDBException;
}
