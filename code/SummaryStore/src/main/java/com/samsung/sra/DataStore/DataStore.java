package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;

/**
 * We implement two stores with this interface:
 *   SummaryStore: time-decayed storage
 *   EnumeratedStore: stores all values explicitly enumerated
 */
public interface DataStore extends AutoCloseable {
    void registerStream(long streamID, Object... params) throws StreamException, RocksDBException;

    void append(long streamID, long timestamp, Object... value) throws StreamException, RocksDBException;

    /** Query operators[operatorNumber] */
    Object query(long streamID, long t0, long t1, int operatorNumber, Object... queryParams)
            throws StreamException, QueryException, RocksDBException;

    // TODO: query operator by name instead of by index

    StreamStatistics getStreamStatistics(long streamID) throws StreamException;

    /** Flush any buffered values into bucket store. Does not flush bucket store to disk */
    default void flush(long streamID) throws RocksDBException, StreamException {}

    @Override
    default void close() throws RocksDBException {}
}
