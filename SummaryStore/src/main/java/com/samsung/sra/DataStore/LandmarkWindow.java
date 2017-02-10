package com.samsung.sra.DataStore;

import java.io.Serializable;
import java.util.SortedMap;
import java.util.TreeMap;

public class LandmarkWindow implements Serializable {
    public final long lwid;
    public long tStart, tEnd;
    public final SortedMap<Long, Object[]> values = new TreeMap<>();

    LandmarkWindow(long lwid, long tStart) {
        this.lwid = lwid;
        this.tStart = tStart;
    }

    public void append(long ts, Object[] value) {
        assert ts >= tStart && (values.isEmpty() || values.lastKey() < ts);
        values.put(ts, value);
    }

    public void close(long ts) {
        assert ts >= tStart && (values.isEmpty() || values.lastKey() <= ts);
        tEnd = ts;
    }

    @Override
    public String toString() {
        return String.format("<landmark-window %d, time range [%d:%d], %d values",
                lwid, tStart, tEnd, values.size());
    }
}
