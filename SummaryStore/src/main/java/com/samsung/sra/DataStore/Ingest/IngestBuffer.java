package com.samsung.sra.DataStore.Ingest;

import java.io.Serializable;

/**
 * Fixed-size buffer of time-value pairs
 */
class IngestBuffer implements Serializable {
    private long[] timestamps;
    private Object[] values;
    private final int capacity;
    private int size = 0;

    IngestBuffer(int capacity) {
        this.capacity = capacity;
        this.timestamps = new long[capacity];
        this.values = new Object[capacity];
    }

    public void append(long ts, Object value) {
        if (size >= capacity) throw new IndexOutOfBoundsException();
        timestamps[size] = ts;
        values[size] = value;
        ++size;
    }

    boolean isFull() {
        return size == capacity;
    }

    int size() {
        return size;
    }

    /**
     * Remove first s elements. After truncate
     *    values[0], values[1], ..., values[size - s - 1] = old values[s], values[s + 1], ..., values[size - 1]*/
    void truncateHead(int s) {
        assert s >= 0;
        if (s == 0) return;
        for (int i = 0; i < size - s; ++i) {
            timestamps[i] = timestamps[i + s];
            values[i] = values[i + s];
        }
        size -= s;
    }

    void clear() {
        size = 0;
    }

    long getTimestamp(int pos) {
        if (pos < 0 || pos >= size) throw new IndexOutOfBoundsException();
        return timestamps[pos];
    }

    Object getValue(int pos) {
        if (pos < 0 || pos >= size) throw new IndexOutOfBoundsException();
        return values[pos];
    }
}
