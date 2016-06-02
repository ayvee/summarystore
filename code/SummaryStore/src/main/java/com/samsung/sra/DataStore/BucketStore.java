package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;

import java.io.Serializable;

/**
 * Underlying key-value store holding all the buckets. Two implementations:
 *      RocksDBBucketStore
 *      MainMemoryBucketStore
 */
interface BucketStore extends AutoCloseable {
    Bucket getBucket(long streamID, long bucketID, boolean delete) throws RocksDBException;

    default Bucket getBucket(long streamID, long bucketID) throws RocksDBException {
        return getBucket(streamID, bucketID, false);
    }

    void putBucket(long streamID, long bucketID, Bucket bucket) throws RocksDBException;

    Serializable getMetadata() throws RocksDBException;

    void putMetadata(Serializable indexes) throws RocksDBException;

    @Override
    void close() throws RocksDBException;
}
