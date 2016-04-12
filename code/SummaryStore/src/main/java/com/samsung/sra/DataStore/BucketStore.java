package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;

/**
 * Key-value store holding all the buckets. Two implementations:
 *  RocksDBBucketStore
 *  MainMemoryBucketStore
 */
interface BucketStore {
    Bucket getBucket(StreamID streamID, BucketID bucketID, boolean delete) throws RocksDBException;

    void putBucket(StreamID streamID, BucketID bucketID, Bucket bucket) throws RocksDBException;

    Object getIndexes() throws RocksDBException;

    void putIndexes(Object indexes) throws RocksDBException;

    void close() throws RocksDBException;
}
