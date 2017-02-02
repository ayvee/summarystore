package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;

import java.io.Serializable;
import java.util.Map;

/**
 * Underlying key-value store holding all the buckets. Two implementations:
 *      RocksDBBackingStore
 *      MainMemoryBackingStore
 */
interface BackingStore extends AutoCloseable {
    Bucket getBucket(StreamManager streamManager, long bucketID) throws RocksDBException;

    Bucket deleteBucket(StreamManager streamManager, long bucketID) throws RocksDBException;

    void putBucket(StreamManager streamManager, long bucketID, Bucket bucket) throws RocksDBException;

    Serializable getMetadata() throws RocksDBException;

    void putMetadata(Serializable indexes) throws RocksDBException;

    default void warmupCache(Map<Long, StreamManager> streamManagers) throws RocksDBException {}

    /** flush cache to disk */
    default void flushCache(StreamManager streamManager) throws RocksDBException {}

    @Override
    void close() throws RocksDBException;
}
