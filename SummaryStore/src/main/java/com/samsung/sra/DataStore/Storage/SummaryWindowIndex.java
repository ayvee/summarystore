package com.samsung.sra.DataStore.Storage;

import com.samsung.sra.DataStore.SummaryWindow;

import java.io.Serializable;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * In-memory index over summary windows. Assumes windows are identified by their start timestamp */
public class SummaryWindowIndex implements Serializable {
    private final NavigableMap<Long, Long> windowCEnds = new TreeMap<>();

    void put(SummaryWindow window) {
        windowCEnds.put(window.ts, window.ce);
    }

    void remove(long swid) {
        windowCEnds.remove(swid);
    }

    Stream<Long> getOverlappingSWIDs(long t0, long t1) {
        if (windowCEnds.isEmpty()) return Stream.empty();
        Long l = windowCEnds.floorKey(t0); // first window with tstart <= t0
        if (l == null) l = windowCEnds.firstKey();
        Long r = windowCEnds.higherKey(t1); // first window with tstart > t1
        if (r == null) r = windowCEnds.lastKey() + 1;
        return windowCEnds.subMap(l, true, r, false).keySet().stream();
    }

    Long getCStart(Long swid) {
        if (swid == null || !windowCEnds.containsKey(swid)) return null;
        Map.Entry<Long, Long> lowerEntry = windowCEnds.lowerEntry(swid);
        if (lowerEntry != null) {
            return lowerEntry.getValue() + 1;
        } else {
            assert swid.equals(windowCEnds.firstKey());
            return 0L;
        }
    }

    Long getCEnd(Long swid) {
        if (swid == null || !windowCEnds.containsKey(swid)) return null;
        return windowCEnds.get(swid);
    }

    Long getCount(Long swid) {
        Long cs = getCStart(swid), ce = getCEnd(swid);
        return cs != null && ce != null ? ce - cs + 1 : null;
    }

    Long getPrevSWID(Long swid) {
        if (swid == null || !windowCEnds.containsKey(swid)) return null;
        Map.Entry<Long, Long> lowerEntry = windowCEnds.lowerEntry(swid);
        return lowerEntry != null ? lowerEntry.getValue() : null;
    }

    Long getNextSWID(Long swid) {
        if (swid == null || !windowCEnds.containsKey(swid)) return null;
        Map.Entry<Long, Long> higherEntry = windowCEnds.higherEntry(swid);
        return higherEntry != null ? higherEntry.getValue() : null;
    }

    public long getNumWindows() {
        return windowCEnds.size();
    }
}
