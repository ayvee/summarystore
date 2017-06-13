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
