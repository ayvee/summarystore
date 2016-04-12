package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;

/**
 * We implement two stores with this interface:
 *   SummaryStore: time-decayed storage
 *   EnumeratedStore: stores all values explicitly enumerated
 */
public interface DataStore extends AutoCloseable {
    // TODO: allow configuring the choice of bucket data structure for each stream (set at registration time)
    void registerStream(StreamID streamID, WindowingMechanism windowingMechanism) throws StreamException, RocksDBException;

    Object query(StreamID streamID, Timestamp t0, Timestamp t1, QueryType queryType, Object[] queryParams)
            throws StreamException, QueryException, RocksDBException;

    void append(StreamID streamID, Timestamp ts, Object value) throws StreamException, RocksDBException;

    @Override
    void close() throws RocksDBException;

    long getStoreSizeInBytes();

    long getStreamAge(StreamID streamID) throws StreamException;

    long getStreamLength(StreamID streamID) throws StreamException;
}
