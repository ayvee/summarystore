package com.samsung.sra.DataStore;

import java.util.*;

class CountBasedWBMH implements WindowingMechanism {
    private final WindowLengthsGenerator windowLengths;
    private final TreeSet<Long> windowStartMarkers;

    CountBasedWBMH(WindowLengthsGenerator windowLengths) {
        this.windowLengths = windowLengths;
        windowStartMarkers = new TreeSet<Long>();
        windowStartMarkers.add(0L);
    }

    private void insertEnoughMarkersToCover(long numElements) {
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
        //System.out.println("window_containing([" + cStart + ", " + cEnd + "], N = " + N + ") is " + ret);
    }

    @Override
    public List<SummaryStore.BucketModification> computeModifications(
            LinkedHashMap<BucketID, BucketMetadata> existingBuckets,
            long numValuesSoFar, Timestamp lastInsertedTimestamp,
            Timestamp newValueTimestamp, Object newValue, boolean isLandmarkValue) {
        if (lastInsertedTimestamp == null) assert numValuesSoFar == 0;
        if (existingBuckets.isEmpty()) assert lastInsertedTimestamp == null;

        insertEnoughMarkersToCover(numValuesSoFar + 1);

        BucketID newBucketID; // id of potential new bucket of size 1 holding the new element
        LinkedHashMap<BucketID, BucketMetadata> baseBuckets = new LinkedHashMap<BucketID, BucketMetadata>();
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

        // For each base bucket:
        //   if the bucket is completely contained inside a window, map it to that window's ID,
        //   else map it to null
        LinkedHashMap<BucketID, Long> containingWindowID = new LinkedHashMap<BucketID, Long>();
        BucketID prevID = null; Long prevCStart = null;
        for (Map.Entry<BucketID, BucketMetadata> entry: baseBuckets.entrySet()) {
            BucketID currID = entry.getKey();
            BucketMetadata currMD = entry.getValue();
            if (prevID != null) {
                Long prevCEnd = currMD.cStart - 1;
                containingWindowID.put(prevID, idOfContainingWindow(prevCStart, prevCEnd, numValuesSoFar + 1));
            }
            prevID = currID;
            prevCStart = currMD.cStart;
        }
        // The last bucket remains unprocessed: the one with newBucketID
        containingWindowID.put(newBucketID, 0L);

        // Recall that WBMH = merge all buckets that are inside the same window. To determine merges,
        // we will group containingWindowID by value
        LinkedHashMap<Long, List<BucketID>> potentialMergeLists = new LinkedHashMap<Long, List<BucketID>>();
        for (Map.Entry<BucketID, Long> entry: containingWindowID.entrySet()) {
            BucketID id = entry.getKey();
            Long marker = entry.getValue();
            if (marker != null) {
                if (!potentialMergeLists.containsKey(marker)) {
                    potentialMergeLists.put(marker, new ArrayList<BucketID>());
                }
                potentialMergeLists.get(marker).add(id);
            }
        }

        // Now process the merge lists to generate the sequence of bucket modifications needed
        List<SummaryStore.BucketModification> bucketModifications = new ArrayList<SummaryStore.BucketModification>();
        // was the newly created bucket (newBucketID) merged into an existing bucket?
        boolean newBucketWasMerged = false;
        for (List<BucketID> mergeList: potentialMergeLists.values()) {
            assert mergeList != null && !mergeList.isEmpty();
            if (mergeList.size() > 1) {
                BucketID target = null;
                List<BucketID> srcs = new ArrayList<BucketID>();
                for (BucketID id: mergeList) {
                    if (id == newBucketID) {
                       if (target != null) {
                           newBucketWasMerged = true;
                       }
                    } else {
                        if (target == null) {
                            target = id;
                        } else {
                            srcs.add(id);
                        }
                    }
                }
                if (target != null && !srcs.isEmpty()) {
                    bucketModifications.add(new SummaryStore.BucketMergeModification(target, srcs));
                }
            }
        }
        if (!newBucketWasMerged) { // need to create new base bucket of size 1 holding the latest element
            bucketModifications.add(new SummaryStore.BucketCreateModification(baseBuckets.get(newBucketID)));
        }

        return bucketModifications;
    }
}
