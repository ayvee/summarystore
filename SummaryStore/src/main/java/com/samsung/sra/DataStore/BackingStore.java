package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;

import java.io.Serializable;
import java.util.Map;

/**
 * Underlying key-value store holding all the windows. Two implementations:
 *      RocksDBBackingStore
 *      MainMemoryBackingStore
 */
interface BackingStore extends AutoCloseable {
    SummaryWindow getSummaryWindow(StreamManager streamManager, long swid) throws RocksDBException;

    SummaryWindow deleteSummaryWindow(StreamManager streamManager, long swid) throws RocksDBException;

    void putSummaryWindow(StreamManager streamManager, long swid, SummaryWindow window) throws RocksDBException;

    Serializable getMetadata() throws RocksDBException;

    void putMetadata(Serializable indexes) throws RocksDBException;

    default void warmupCache(Map<Long, StreamManager> streamManagers) throws RocksDBException {}

    /** flush cache to disk */
    default void flushCache(StreamManager streamManager) throws RocksDBException {}

    @Override
    void close() throws RocksDBException;
}
