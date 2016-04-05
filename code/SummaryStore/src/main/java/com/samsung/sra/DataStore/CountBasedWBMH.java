package com.samsung.sra.DataStore;

import java.util.*;

public class CountBasedWBMH implements WindowingMechanism {
    private final WindowLengthSequence windowLengths;
    private final TreeSet<Long> windowStartMarkers;

    public CountBasedWBMH(WindowLengthSequence windowLengths) {
        this.windowLengths = windowLengths;
        windowStartMarkers = new TreeSet<>();
        windowStartMarkers.add(0L);
    }

    private synchronized void insertEnoughMarkersToCover(long numElements) {
        Long lastStart = windowStartMarkers.last();
        while (lastStart <= numElements) {
            long length = windowLengths.nextWindowLength();
            assert length > 0; // TODO: && length >= maxLengthSoFar
            lastStart += length;
            windowStartMarkers.add(lastStart);
        }
    }

    /**
     * If the interval [cStart, cEnd] is completely contained inside some window after
     * N values have been inserted, return a unique ID representing that window, else
     * return null.
     *
     * We use the window's start marker as our unique ID
     */
    private Long idOfContainingWindow(long cStart, long cEnd, long N) {
        assert 0 <= cStart && cStart <= cEnd && cEnd < N;
        long lAge = N-1 - cEnd, rAge = N-1 - cStart;
        Long lMarker = windowStartMarkers.floor(lAge), rMarker = windowStartMarkers.floor(rAge);
        assert lMarker != null && rMarker != null;
        return lMarker.equals(rMarker) ? lMarker : null;
    }

    @Override
    public List<SummaryStore.BucketModification> computeModifications(
            TreeMap<BucketID, BucketMetadata> existingBuckets,
            long numValuesSoFar, Timestamp lastInsertedTimestamp,
            Timestamp newValueTimestamp, Object newValue, boolean isLandmarkValue) {
        if (lastInsertedTimestamp == null) assert numValuesSoFar == 0;
        if (existingBuckets.isEmpty()) assert lastInsertedTimestamp == null;

        insertEnoughMarkersToCover(numValuesSoFar + 1);

        BucketID newBucketID; // id of potential new bucket of size 1 holding the new element
        LinkedHashMap<BucketID, BucketMetadata> baseBuckets = new LinkedHashMap<>();
        if (existingBuckets.isEmpty()) {
            newBucketID = new BucketID(0);
            baseBuckets.put(newBucketID, new BucketMetadata(newBucketID, new Timestamp(0), 0L, false));
        } else {
            newBucketID = null;
            for (BucketMetadata md: existingBuckets.values()) {
                newBucketID = md.bucketID;
                if (!md.isLandmark) {
                    baseBuckets.put(md.bucketID, md);
                }
            }
            assert newBucketID != null;
            newBucketID = newBucketID.nextBucketID();
            baseBuckets.put(newBucketID, new BucketMetadata(newBucketID, newValueTimestamp, numValuesSoFar, false));
        }

        // Recall that WBMH = merge all buckets that are inside the same window. To determine
        // potential merges, we will now group all buckets by the ID of their containing window
        LinkedHashMap<Long, List<BucketID>> potentialMergeLists = new LinkedHashMap<>();
        BucketID prevID = null; Long prevCStart = null;
        for (Map.Entry<BucketID, BucketMetadata> entry: baseBuckets.entrySet()) {
            BucketID currID = entry.getKey();
            BucketMetadata currMD = entry.getValue();
            if (prevID != null) {
                Long prevCEnd = currMD.cStart - 1;
                Long windowID = idOfContainingWindow(prevCStart, prevCEnd, numValuesSoFar + 1);
                if (windowID != null) {
                    List<BucketID> dst = potentialMergeLists.get(windowID);
                    if (dst == null) {
                        potentialMergeLists.put(windowID, (dst = new ArrayList<>()));
                    }
                    dst.add(prevID);
                }
                // else the bucket is not completely contained inside a window
            }
            prevID = currID;
            prevCStart = currMD.cStart;
        }
        // At this point the last bucket remains unprocessed: the one with newBucketID. This bucket
        // will need to be created iff there is no existing base bucket contained inside the newest window
        boolean createNewBucket = !potentialMergeLists.containsKey(0L);

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
            bucketModifications.add(new SummaryStore.BucketCreateModification(baseBuckets.get(newBucketID)));
        }

        return bucketModifications;
    }
}
