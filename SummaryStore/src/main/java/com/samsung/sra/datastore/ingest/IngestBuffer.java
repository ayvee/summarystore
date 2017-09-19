package com.samsung.sra.datastore.ingest;

import java.io.Serializable;

/**
 * Fixed-size buffer of time-value pairs
 */
interface IngestBuffer extends AutoCloseable, Serializable {
    void append(long ts, Object value);

    boolean isFull();

    int size();

    /**
     * Remove first s elements. After truncate
     *    values[0], values[1], ..., values[size - s - 1] = old values[s], values[s + 1], ..., values[size - 1]*/
    void truncateHead(int s);

    void clear();

    long getTimestamp(int pos);

    Object getValue(int pos);

    @Override
    default void close() {}
}
