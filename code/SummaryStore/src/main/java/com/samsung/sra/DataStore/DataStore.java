package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;

/**
 * We implement two stores with this interface:
 *   SummaryStore: time-decayed storage with landmarks
 *   EnumeratedStore: stores all values explicitly enumerated
 */
public interface DataStore {
    // TODO: allow configuring the choice of bucket data structure for each stream (set at registration time)
    void registerStream(StreamID streamID) throws StreamException, RocksDBException;

    Object query(StreamID streamID,
                 Timestamp t0, Timestamp t1, Bucket.QueryType queryType, Object[] queryParams)
            throws StreamException, QueryException, RocksDBException;

    void append(StreamID streamID,
                Timestamp ts, Object value, boolean landmarkStartsHere, boolean landmarkEndsHere) throws StreamException, LandmarkEventException, RocksDBException;

    void close();

    long getStoreSizeInBytes();

    /** desired functions
     *
     *
     * long getStreamAgeInSeconds(StreamID streamID);
     *
     * long getStreamLength(StreamID streamID);
     *
     */
}
