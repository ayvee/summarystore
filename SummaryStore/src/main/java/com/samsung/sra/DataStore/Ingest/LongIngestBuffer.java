package com.samsung.sra.DataStore.Ingest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xerial.larray.LLongArray;
import xerial.larray.japi.LArrayJ;

import java.util.concurrent.atomic.AtomicInteger;

class LongIngestBuffer implements IngestBuffer {
    private static final Logger logger = LoggerFactory.getLogger(LongIngestBuffer.class);

    private LLongArray timestamps, values;
    private final int capacity;
    private int size = 0;
    private final int id;
    private static AtomicInteger num = new AtomicInteger(0);

    LongIngestBuffer(int capacity) {
        this.capacity = capacity;
        this.id = num.incrementAndGet();
        logger.info("ingest buffer {}: about to malloc 2 * long[{}]", this.id, this.capacity);

        this.timestamps = LArrayJ.newLLongArray(capacity);
        this.values = LArrayJ.newLLongArray(capacity);
    }

    @Override
    public void append(long ts, Object value) {
        if (size >= capacity) throw new IndexOutOfBoundsException();
        timestamps.update(size, ts);
        values.update(size, ((Number) value).longValue());
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
            timestamps.update(i, timestamps.apply(i + s));
            values.update(i, values.apply(i + s));
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
        return timestamps.apply(pos);
    }

    @Override
    public Object getValue(int pos) {
        if (pos < 0 || pos >= size) throw new IndexOutOfBoundsException();
        return values.apply(pos);
    }

    @Override
    public void close() {
        logger.info("ingest buffer {}: about to free 2 * long[{}]", this.id, this.capacity);
        timestamps.free();
        values.free();
    }
}
