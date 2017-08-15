package com.samsung.sra.datastore.ingest;

class ObjectIngestBuffer implements IngestBuffer {
    private long[] timestamps;
    private Object[] values;
    private final int capacity;
    private int size = 0;

    ObjectIngestBuffer(int capacity) {
        this.capacity = capacity;
        this.timestamps = new long[capacity];
        this.values = new Object[capacity];
    }

    @Override
    public void append(long ts, Object value) {
        if (size >= capacity) throw new IndexOutOfBoundsException();
        timestamps[size] = ts;
        values[size] = value;
        ++size;
    }

    @Override
    public boolean isFull() {
        return size == capacity;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public void truncateHead(int s) {
        assert s >= 0;
        if (s == 0) return;
        for (int i = 0; i < size - s; ++i) {
            timestamps[i] = timestamps[i + s];
            values[i] = values[i + s];
        }
        size -= s;
    }

    @Override
    public void clear() {
        size = 0;
    }

    @Override
    public long getTimestamp(int pos) {
        if (pos < 0 || pos >= size) throw new IndexOutOfBoundsException();
        return timestamps[pos];
    }

    @Override
    public Object getValue(int pos) {
        if (pos < 0 || pos >= size) throw new IndexOutOfBoundsException();
        return values[pos];
    }
}
