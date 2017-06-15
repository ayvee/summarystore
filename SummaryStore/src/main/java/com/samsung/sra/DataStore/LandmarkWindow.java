package com.samsung.sra.DataStore;

import java.io.Serializable;
import java.util.SortedMap;
import java.util.TreeMap;

public class LandmarkWindow implements Serializable {
    public long ts, te;
    public final SortedMap<Long, Object> values = new TreeMap<>();

    public LandmarkWindow(long ts) {
        this.ts = ts;
    }

    public void append(long timestamp, Object value) {
        assert timestamp >= ts && (values.isEmpty() || values.lastKey() < timestamp);
        values.put(timestamp, value);
    }

    public void close(long timestamp) {
        assert timestamp >= ts && (values.isEmpty() || values.lastKey() <= timestamp);
        te = timestamp;
    }

    @Override
    public String toString() {
        return String.format("<landmark-window: time range [%d:%d], %d values", ts, te, values.size());
    }
}
