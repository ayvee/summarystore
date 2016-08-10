package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;

/**
 * We implement two stores with this interface:
 *   SummaryStore: time-decayed storage
 *   EnumeratedStore: stores all values explicitly enumerated
 */
public interface DataStore extends AutoCloseable {
    void registerStream(long streamID,
                        WindowingMechanism windowingMechanism,
                        WindowOperator operators[]) throws StreamException, RocksDBException;

    /** Query operators[operatorNumber] */
    Object query(long streamID, long t0, long t1, int operatorNumber, Object... queryParams)
            throws StreamException, QueryException, RocksDBException;

    // TODO: query operator by name instead of by index

    void append(long streamID, long timestamp, Object value) throws StreamException, RocksDBException;

    /** Flush any buffered values into bucket store. Does not flush buffer store to disk */
    default void flush(long streamID) throws RocksDBException, StreamException {}

    // FIXME: do we need a flush to disk API?

    @Override
    void close() throws RocksDBException;

    // TODO: move into StreamStatistics
    long getStoreSizeInBytes();

    StreamStatistics getStreamStatistics(long streamID) throws StreamException;
}
