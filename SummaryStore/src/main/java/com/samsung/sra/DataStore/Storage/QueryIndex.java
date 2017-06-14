package com.samsung.sra.DataStore.Storage;

import java.io.Serializable;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;

/** In-memory index over window time-starts, used to support query() */
class QueryIndex implements Serializable {
    private final SortedSet<Long> tStarts = Collections.synchronizedSortedSet(new TreeSet<>());

    void add(long tStart) {
        tStarts.add(tStart);
    }

    void remove(long tStart) {
        tStarts.remove(tStart);
    }

    /**
     * Get windows that might overlap [t0, t1], specifically
     *     [last window with tStart < t0, ..., last window with tStart <= t1]
     * Very first window may not overlap [t0, t1], depending on its tEnd; should probably use this function as
     *    getOverlappingWindowIDs(t0, t1).map(windowGetter).filter(w -> w.te >= t0)
     */

    Stream<Long> getOverlappingWindowIDs(long t0, long t1) {
        if (tStarts.isEmpty()) return Stream.empty();
        // respectively, all windows with tStart < t0 and > t1
        SortedSet<Long> lt = tStarts.headSet(t0), gt = tStarts.tailSet(t1 + 1);
        long l = !lt.isEmpty() ? lt.last() : tStarts.first();
        long r = !gt.isEmpty() ? gt.first() : tStarts.last() + 1;
        return tStarts.subSet(l, r).stream();
    }

    long getNumWindows() {
        return tStarts.size();
    }
}
