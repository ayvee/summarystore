package com.samsung.sra.DataStore;

import java.util.*;

/**
 * Older implementation of WBMH that, after each append, starts from scratch to check
 * which buckets are in the same window. Slower than the newer implementation, but
 * potentially more memory-efficient, because we only build out the windowing to cover
 * precisely the elements which have been inserted.
 */
public class SlowCountBasedWBMH implements WindowingMechanism {
    private long N = 0;
    private BucketID lastBucketID = null;

    private static class BucketInfo {
        long cStart, cEnd;

        BucketInfo(long cStart, long cEnd) {
            this.cStart = cStart;
            this.cEnd = cEnd;
        }
    }

    // NOTE: expected to be sorted by BucketID, make sure inserts occur in order.
    //       Not using TreeMap because it was at least 2x slower in experiments
    private final LinkedHashMap<BucketID, BucketInfo> bucketsInfo = new LinkedHashMap<>();

    private final WindowLengths windowLengths;
    private final TreeMap<Long, Integer> windowStartMarkers = new TreeMap<>();
    private final List<Long> windowStartMarkersList = new ArrayList<>();

    public SlowCountBasedWBMH(WindowLengths windowLengths) {
        this.windowLengths = windowLengths;
        windowStartMarkers.put(0L, 0);
        windowStartMarkersList.add(0L);
    }

    private synchronized void updateWindowingToCover(long numElements) {
        Long lastStart = windowStartMarkers.lastKey();
        while (lastStart <= numElements) {
            long length = windowLengths.nextWindowLength();
            assert length > 0; // TODO: && length >= maxLengthSoFar
            lastStart += length;
            windowStartMarkers.put(lastStart, windowStartMarkers.size());
            windowStartMarkersList.add(lastStart);
        }
    }

    /**
     * If the interval [cStart, cEnd] is completely contained inside some window after
     * N values have been inserted, return a unique ID representing that window, else
     * return null.
     *
     * We use the window's start marker as our unique ID
     */
    /*private Long idOfContainingWindow(long cStart, long cEnd, long N) {
        assert 0 <= cStart && cStart <= cEnd && cEnd < N;
        long lAge = N-1 - cEnd, rAge = N-1 - cStart;
        Long lMarker = windowStartMarkersSet.floor(lAge), rMarker = windowStartMarkersSet.floor(rAge);
        assert lMarker != null && rMarker != null;
        return lMarker.equals(rMarker) ? lMarker : null;
    }*/

    // Invariant: markerRi == index of first window start marker > r
    private int markerRi = -1;

    private final Object idOfFirstWindow = 0;

    private void resetIdOfContainingWindowInternalState() {
        markerRi = -1;
    }

    private Object idOfContainingWindow(long cStart, long cEnd, long N) {
        assert 0 <= cStart && cStart <= cEnd && cEnd < N;
        long l = N-1 - cEnd, r = N-1 - cStart;
        if (markerRi == -1) {
            markerRi = windowStartMarkers.higherEntry(r).getValue();
        } else {
            assert markerRi == windowStartMarkers.higherEntry(r).getValue();
        }
        int markerLi = markerRi - 1;
        while (windowStartMarkersList.get(markerLi) > l)
            --markerLi;
        // now markerLi == index of first window start marker <= l
        Integer ret = (markerLi == markerRi - 1) ? markerLi : null;
        // next bucket's r will be l - 1
        if (l == 0) {
            markerRi = -1;
        } else {
            markerRi = l-1 < windowStartMarkersList.get(markerLi) ? markerLi : markerLi + 1;
        }
        return ret;
    }

    @Override
    public List<SummaryStore.BucketModification> computeModifications(Timestamp newValueTimestamp, Object newValue) {
        ++N;
        updateWindowingToCover(N);
        resetIdOfContainingWindowInternalState();

        if (lastBucketID != null) {
            assert N >= 2;
            bucketsInfo.get(lastBucketID).cEnd = N-2;
        } else {
            assert N == 1;
        }

        List<SummaryStore.BucketModification> bucketModifications = new ArrayList<>();

        // Recall that WBMH = merge all buckets that are inside the same window. To determine
        // potential merges, we will group all buckets by the ID of their containing window
        Object prevWindowID = null;
        BucketID mergee = null;
        BucketInfo mergeeInfo = null;
        List<BucketID> merged = null;
        for (Iterator<Map.Entry<BucketID, BucketInfo>> iter = bucketsInfo.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry<BucketID, BucketInfo> entry = iter.next();
            BucketID bucketID = entry.getKey();
            BucketInfo bucketInfo = entry.getValue();
            Object currWindowID = idOfContainingWindow(bucketInfo.cStart, bucketInfo.cEnd, N);
            if (currWindowID == null) {
                // Flush any existing merge into the list of modifications
                processPotentialMerge(bucketModifications, mergee, merged);
                mergee = null;
                merged = null;
            } else {
                if (!currWindowID.equals(prevWindowID)) {
                    // 1. Flush any existing merge into the list of modifications
                    processPotentialMerge(bucketModifications, mergee, merged);
                    // 2. This bucket (i.e. prevBucketID) is potentially the head of a merge sequence, start tracking
                    mergee = bucketID;
                    mergeeInfo = bucketsInfo.get(mergee);
                    merged = null;
                } else {
                    // this should be merged into mergee
                    if (merged == null) {
                        merged = new ArrayList<>();
                    }
                    merged.add(bucketID);
                    assert mergeeInfo.cEnd == bucketInfo.cStart - 1;
                    mergeeInfo.cEnd = bucketInfo.cEnd;
                    iter.remove();
                }
            }
            prevWindowID = currWindowID;
        }
        processPotentialMerge(bucketModifications, mergee, merged);
        // At this point the last bucket remains unprocessed: the one with newBucketID. This bucket
        // will need to be created iff there is no existing base bucket contained inside the newest window
        if (!idOfFirstWindow.equals(prevWindowID)) {
            BucketID newBucketID = lastBucketID != null ? lastBucketID.nextBucketID() : new BucketID(0);
            // semantics decision: we start streams at T = 0, as opposed to T = timestamp of first ever inserted element
            Timestamp newBucketTStart = lastBucketID != null ? newValueTimestamp : new Timestamp(0);
            bucketModifications.add(new SummaryStore.BucketCreateModification(newBucketID, newBucketTStart, N-1));
            bucketsInfo.put(newBucketID, new BucketInfo(N-1, -1));
            lastBucketID = newBucketID;
        } else {
            // no new bucket, just reset the last bucket's end count to infinity
            bucketsInfo.get(lastBucketID).cEnd = -1;
        }

        return bucketModifications;
    }

    private void processPotentialMerge(List<SummaryStore.BucketModification> bucketModifications, BucketID mergee, List<BucketID> merged) {
        if (mergee == null || merged == null || merged.isEmpty()) {
            return;
        }
        bucketModifications.add(new SummaryStore.BucketMergeModification(mergee, merged));
    }
}
