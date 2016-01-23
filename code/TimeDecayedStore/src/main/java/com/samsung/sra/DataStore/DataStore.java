package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;

import java.util.Collection;
import java.util.List;

/**
 * We implement two stores with this interface:
 *   TimeDecayedStore: implements the time-decay and landmark portions of SummaryStore
 *   EnumeratedStore: stores all values explicitly enumerated
 */
public interface DataStore {
    void registerStream(StreamID streamID) throws StreamException;

    Object query(StreamID streamID, int queryType, int t0, int t1) throws StreamException, QueryException, RocksDBException;

    void append(StreamID streamID, Collection<FlaggedValue> values) throws StreamException, LandmarkEventException, RocksDBException;

    void close();
}
