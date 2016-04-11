package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;

import java.util.HashMap;

/**
 * Key-value store holding the buckets as values. Two implementations:
 *  RocksDBBucketStore
 *  MainMemoryStore
 */
interface BucketStore {
    Bucket getBucket(StreamID streamID, BucketID bucketID, boolean delete) throws RocksDBException;

    void putBucket(StreamID streamID, BucketID bucketID, Bucket bucket) throws RocksDBException;

    Object getIndexes() throws RocksDBException;

    void putIndexes(Object indexes) throws RocksDBException;

    void close() throws RocksDBException;
}
