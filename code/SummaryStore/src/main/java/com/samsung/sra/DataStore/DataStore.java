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

    /** Query the last operator in operators[] that supports the requested queryType */
    //Object query(long streamID, long t0, long t1, String queryType, Object... queryParams)
    //        throws StreamException, QueryException, RocksDBException;

    /** Query operators[operatorNumber] */
    Object query(long streamID, long t0, long t1, int operatorNumber, Object... queryParams)
            throws StreamException, QueryException, RocksDBException;

    void append(long streamID, long timestamp, Object value) throws StreamException, RocksDBException;

    @Override
    void close() throws RocksDBException;

    long getStoreSizeInBytes();

    long getStreamAge(long streamID) throws StreamException;

    long getStreamCount(long streamID) throws StreamException;
}
