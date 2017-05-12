package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;

/**
 * Underlying key-value store holding all the windows. Two implementations:
 *      RocksDBBackingStore
 *      MainMemoryBackingStore
 */
interface BackingStore extends AutoCloseable {
    SummaryWindow getSummaryWindow(StreamManager streamManager, long swid) throws RocksDBException;

    SummaryWindow deleteSummaryWindow(StreamManager streamManager, long swid) throws RocksDBException;

    void putSummaryWindow(StreamManager streamManager, long swid, SummaryWindow window) throws RocksDBException;

    LandmarkWindow getLandmarkWindow(StreamManager streamManager, long lwid) throws RocksDBException;

    void putLandmarkWindow(StreamManager streamManager, long lwid, LandmarkWindow window) throws RocksDBException;

    /** flush all entries for specified stream to disk */
    default void flushCache(StreamManager streamManager) throws RocksDBException {}

    @Override
    void close() throws RocksDBException;
}
