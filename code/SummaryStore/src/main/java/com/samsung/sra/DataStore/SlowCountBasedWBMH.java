package com.samsung.sra.DataStore;

import java.util.*;

/**
 * Older implementation of WBMH that, after each append, starts from scratch to check
 * which buckets are in the same window. Slower than the newer implementation, but
 * potentially more memory-efficient, because we only build out the windowing to cover
 * precisely the elements which have been inserted.
 */
public class SlowCountBasedWBMH implements WindowingMechanism {
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

        markerRi = -1; // FIXME
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

    /* FIXME: the way we maintain markerRi only works if we're single-threaded (no multiple streams)
              To fix, pass markerRi around as a continuation */
    // Invariant: markerRi == index of first window start marker > r
    private int markerRi = -1;

    private final Object idOfFirstWindow = 0;

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
    public List<SummaryStore.BucketModification> computeModifications(
            TreeMap<BucketID, BucketMetadata> existingBuckets,
            long numValuesSoFar, Timestamp lastInsertedTimestamp,
            Timestamp newValueTimestamp, Object newValue) {
        if (lastInsertedTimestamp == null) assert numValuesSoFar == 0;
        if (existingBuckets.isEmpty()) assert lastInsertedTimestamp == null;

        updateWindowingToCover(numValuesSoFar + 1);

        BucketID newBucketID; // id of potential new bucket of size 1 holding the new (t, v) pair
        BucketMetadata newBucketMD;
        if (existingBuckets.isEmpty()) {
            newBucketID = new BucketID(0);
            newBucketMD = new BucketMetadata(newBucketID, new Timestamp(0), 0L);
        } else {
            newBucketID = existingBuckets.lastKey().nextBucketID();
            newBucketMD = new BucketMetadata(newBucketID, newValueTimestamp, numValuesSoFar);
        }
        // WARNING: modifying existingBuckets here (we weren't supposed to, per the interface contract).
        //          Need to undo before returning
        existingBuckets.put(newBucketID, newBucketMD);

        List<SummaryStore.BucketModification> bucketModifications = new ArrayList<>();

        // FIXME: used to be written much more cleanly; turned into a messy multiloop after optimizing
        BucketID prevBucketID = null; // since we don't maintain end markers, we need
        Long prevCStart = null;       // to go over buckets two at a time to find them
        // Recall that WBMH = merge all buckets that are inside the same window. To determine
        // potential merges, we will group all buckets by the ID of their containing window
        Object prevWindowID = null;
        BucketID mergee = null;
        List<BucketID> merged = null;
        for (Map.Entry<BucketID, BucketMetadata> entry: existingBuckets.entrySet()) {
            BucketID currBucketID = entry.getKey();
            BucketMetadata currMD = entry.getValue();
            if (prevBucketID != null) {
                Long prevCEnd = currMD.cStart - 1;
                Object currWindowID = idOfContainingWindow(prevCStart, prevCEnd, numValuesSoFar + 1);
                if (currWindowID == null) {
                    // Flush any existing merge into the list of modifications
                    if (mergee != null && merged != null)
                        bucketModifications.add(new SummaryStore.BucketMergeModification(mergee, merged));
                    mergee = null;
                    merged = null;
                } else {
                    if (!currWindowID.equals(prevWindowID)) {
                        // 1. Flush any existing merge into the list of modifications
                        if (mergee != null && merged != null)
                            bucketModifications.add(new SummaryStore.BucketMergeModification(mergee, merged));
                        // 2. This bucket (i.e. prevBucketID) is potentially the head of a merge sequence, start tracking
                        mergee = prevBucketID;
                        merged = null;
                    } else {
                        // this should be merged into mergee
                        if (merged == null) {
                            merged = new ArrayList<>();
                        }
                        merged.add(prevBucketID);
                    }
                }
                prevWindowID = currWindowID;
            }
            prevBucketID = currBucketID;
            prevCStart = currMD.cStart;
        }
        if (mergee != null && merged != null)
            bucketModifications.add(new SummaryStore.BucketMergeModification(mergee, merged));
        // At this point the last bucket remains unprocessed: the one with newBucketID. This bucket
        // will need to be created iff there is no existing base bucket contained inside the newest window
        if (!idOfFirstWindow.equals(prevWindowID))
            bucketModifications.add(new SummaryStore.BucketCreateModification(newBucketMD));

        existingBuckets.remove(newBucketID);

        return bucketModifications;
    }
}
