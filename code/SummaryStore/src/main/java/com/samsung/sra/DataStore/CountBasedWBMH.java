package com.samsung.sra.DataStore;

import java.util.*;

public class CountBasedWBMH implements WindowingMechanism {
    private final WindowLengthSequence windowLengths;
    private final TreeMap<Long, Integer> windowStartMarkers = new TreeMap<>();
    private final List<Long> windowStartMarkersList = new ArrayList<>();

    public CountBasedWBMH(WindowLengthSequence windowLengths) {
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

    private static Object idOfFirstWindow = 0;

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
        BucketMetadata newBucketMetadata;
        // WARNING: modifying existingBuckets here (we weren't supposed to, per the interface contract).
        //          Need to undo before returning
        if (existingBuckets.isEmpty()) {
            newBucketID = new BucketID(0);
            newBucketMetadata = new BucketMetadata(newBucketID, new Timestamp(0), 0L);
        } else {
            newBucketID = existingBuckets.lastKey().nextBucketID();
            newBucketMetadata = new BucketMetadata(newBucketID, newValueTimestamp, numValuesSoFar);
        }
        existingBuckets.put(newBucketID, newBucketMetadata);

        // Recall that WBMH = merge all buckets that are inside the same window. To determine
        // potential merges, we will now group all buckets by the ID of their containing window
        LinkedHashMap<Object, List<BucketID>> potentialMergeLists = new LinkedHashMap<>();
        BucketID prevID = null; Long prevCStart = null;
        for (Map.Entry<BucketID, BucketMetadata> entry: existingBuckets.entrySet()) {
            BucketID currID = entry.getKey();
            BucketMetadata currMD = entry.getValue();
            if (prevID != null) {
                Long prevCEnd = currMD.cStart - 1;
                Object windowID = idOfContainingWindow(prevCStart, prevCEnd, numValuesSoFar + 1);
                if (windowID != null) {
                    List<BucketID> dst = potentialMergeLists.get(windowID);
                    if (dst == null)
                        potentialMergeLists.put(windowID, (dst = new ArrayList<>()));
                    dst.add(prevID);
                }
                // else the bucket is not completely contained inside a window
            }
            prevID = currID;
            prevCStart = currMD.cStart;
        }
        // At this point the last bucket remains unprocessed: the one with newBucketID. This bucket
        // will need to be created iff there is no existing base bucket contained inside the newest window
        boolean createNewBucket = !potentialMergeLists.containsKey(idOfFirstWindow);

        // Now process the merge lists to generate the sequence of bucket modifications needed
        List<SummaryStore.BucketModification> bucketModifications = new ArrayList<>();
        for (List<BucketID> mergeList: potentialMergeLists.values()) {
            assert mergeList != null && !mergeList.isEmpty();
            if (mergeList.size() > 1) {
                BucketID target = null; List<BucketID> srcs = new ArrayList<>();
                for (BucketID id: mergeList) {
                    assert id != newBucketID;
                    if (target == null)
                        target = id;
                    else
                        srcs.add(id);
                }
                bucketModifications.add(new SummaryStore.BucketMergeModification(target, srcs));
            }
        }
        if (createNewBucket) {
            bucketModifications.add(new SummaryStore.BucketCreateModification(newBucketMetadata));
        }
        existingBuckets.remove(newBucketID);

        return bucketModifications;
    }
}
