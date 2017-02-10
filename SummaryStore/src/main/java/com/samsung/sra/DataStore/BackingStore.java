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

    /* *** BEGIN COLUMN STORE OPTIMIZATIONS *** */
    /* Columnar backing stores should override the following two functions*/

    default boolean isColumnar() {
        return false;
    }

    /**
     * Instead of returning the whole window with id swid, return just the window header + the aggregate with
     * specified index. returnValue.aggregates.length will be 1 instead of number of operators
     *
     * Will only be called if isColumnar() returns true
     */
    default SummaryWindow getSummaryWindow(StreamManager streamManager, long swid, int aggregateIdx)
            throws RocksDBException {
        throw new IllegalStateException("placeholder code, should not be called");
    }

    /* *** END COLUMN STORE OPTIMIZATIONS *** */

    SummaryWindow deleteSummaryWindow(StreamManager streamManager, long swid) throws RocksDBException;

    void putSummaryWindow(StreamManager streamManager, long swid, SummaryWindow window) throws RocksDBException;

    LandmarkWindow getLandmarkWindow(StreamManager streamManager, long lwid) throws RocksDBException;

    void putLandmarkWindow(StreamManager streamManager, long lwid, LandmarkWindow window) throws RocksDBException;

    Serializable getMetadata() throws RocksDBException;

    void putMetadata(Serializable indexes) throws RocksDBException;

    default void warmupCache(Map<Long, StreamManager> streamManagers) throws RocksDBException {}

    /** flush cache to disk */
    default void flushCache(StreamManager streamManager) throws RocksDBException {}

    @Override
    void close() throws RocksDBException;
}
