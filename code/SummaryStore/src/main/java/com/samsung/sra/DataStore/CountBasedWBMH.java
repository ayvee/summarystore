package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teneighty.heap.FibonacciHeap;
import org.teneighty.heap.Heap;

import java.util.HashMap;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Newer implementation of WBMH that maintains a priority queue of pending bucket
 * merge operations. Faster than the old implementation, but potentially uses more
 * memory, because it can end up building the windowing out very far into the future.
 * Can be controlled with care, which I've taken some of, but may need more analysis.
 */
public class CountBasedWBMH implements WindowingMechanism {
    private static Logger logger = LoggerFactory.getLogger(CountBasedWBMH.class);
    private final long streamID;
    private final WindowLengths windowLengths;

    private long lastBucketID = -1;
    private long N = 0;
    private final long firstWindowLength; // length of the first window (the one holding the newest element)

    /* Priority queue, mapping each BucketID b_i to the time at which b_{i+1} will be
     * merged into it. Using a Fibonacci heap instead of the Java Collections PriorityQueue
     * because we need an efficient arbitrary-element delete.
     *
     * Why this particular Fibonacci heap implementation?
     * https://gabormakrai.wordpress.com/2015/02/11/experimenting-with-dijkstras-algorithm/
     */
    private final FibonacciHeap<Long, Long> mergeCounts = new FibonacciHeap<>();
    private final HashMap<Long, Heap.Entry<Long, Long>> heapEntries = new HashMap<>();

    public CountBasedWBMH(long streamID, WindowLengths windowLengths) {
        this.streamID = streamID;
        this.windowLengths = windowLengths;
        addWindow((firstWindowLength = windowLengths.nextWindowLength()));
    }

    // maps window length to the start marker of the first window of that length
    private final TreeMap<Long, Long> firstWindowOfLength = new TreeMap<>();
    // all window start markers in an ordered set
    private final TreeSet<Long> windowStartMarkers = new TreeSet<>();

    private long lastWindowStart = 0L, lastWindowLength = 0L;

    private void addWindow(long length) {
        assert length >= lastWindowLength && length > 0;
        lastWindowStart += lastWindowLength;
        if (length > lastWindowLength) firstWindowOfLength.put(length, lastWindowStart);
        windowStartMarkers.add(lastWindowStart);
        lastWindowLength = length;
    }

    /**
     * Add windows until we have one with length >= the specified target. Returns false
     * if the target length isn't achievable
     */
    private boolean addWindowsUntilLength(long targetLength) {
        if (targetLength <= lastWindowLength) {
            // already added, nothing to do
            return true;
        } else {
            return windowLengths.addWindowsUntilLength(targetLength, this::addWindow);
        }
    }

    /**
     * Add windows until we have at least one window marker larger than the target
     */
    private void addWindowsPastMarker(long targetMarker) {
        while (lastWindowStart <= targetMarker) {
            addWindow(windowLengths.nextWindowLength());
        }
    }

    /**
     * Find the smallest N' >= N such that after N' elements have been inserted
     * the interval [Cl, Cr] will be contained inside the same window. Returns
     * -1 if no such N' exists
     */
    private long findMergeCount(long N, long Cl, long Cr) {
        assert 0 <= Cl && Cl <= Cr && Cr < N;
        long l = N-1 - Cr, r = N-1 - Cl, length = Cr - Cl + 1;

        if (!addWindowsUntilLength(length)) {
            return -1;
        }
        long firstMarker = firstWindowOfLength.ceilingEntry(length).getValue();
        if (firstMarker >= l) {
            // l' == firstMarker, where l' := N'-1 - Cr
            return firstMarker + Cr + 1;
        } else {
            // we've already hit the target window length, so [l, r] is either
            // already in the same window or will be once we move into the next window
            addWindowsPastMarker(l);
            long currWindowL = windowStartMarkers.floor(l), currWindowR = windowStartMarkers.higher(l) - 1;
            if (r <= currWindowR) {
                // already in same window
                return N;
            } else {
                assert currWindowR - currWindowL + 1 >= length;
                // need to wait until next window, i.e. l' == currWindowR + 1, where l' := N'-1 - Cr
                return currWindowR + Cr + 2;
            }
        }
    }

    /**
     * Update the priority queue's entry for the merge pair (b0, b1)
     * */
    private void updateMergeCountFor(Bucket b0, Bucket b1) {
        if (b0 == null || b1 == null) return;

        Heap.Entry<Long, Long> existingEntry = heapEntries.remove(b0.thisBucketID);
        if (existingEntry != null) mergeCounts.delete(existingEntry);

        long newMergeCount = findMergeCount(N, b0.cStart, b1.cEnd);
        if (newMergeCount != -1) {
            heapEntries.put(b0.thisBucketID, mergeCounts.insert(newMergeCount, b0.thisBucketID));
        }
    }

    @Override
    public void append(SummaryStore store, long ts, Object value) throws RocksDBException {
        if (logger.isDebugEnabled() && N % 1000000 == 0) {
            logger.debug("length of windowStartMarkers = {}, firstWindowOfLength = {}, mergeCounts = {}",
                    windowStartMarkers.size(), firstWindowOfLength.size(), mergeCounts.getSize());
        }
        ++N;

        while (!mergeCounts.isEmpty() && mergeCounts.getMinimum().getKey() <= N) {
            Heap.Entry<Long, Long> entry = mergeCounts.extractMinimum();
            Heap.Entry<Long, Long> removed = heapEntries.remove(entry.getValue());
            assert removed == entry;
            Bucket b0 = store.getBucket(streamID, entry.getValue());
            // We will now merge b0's successor b1 into b0. We also need to update b{-1}'s and
            // b2's prev and next pointers and b{-1} and b0's heap entries
            assert b0.nextBucketID != -1;
            Bucket b1 = store.getBucket(streamID, b0.nextBucketID, true); // note: this deletes b1 from bucketStore
            Bucket b2 = b1.nextBucketID == -1 ? null : store.getBucket(streamID, b1.nextBucketID);
            Bucket bm1 = b0.prevBucketID == -1 ? null : store.getBucket(streamID, b0.prevBucketID); // b{-1}

            b0.merge(b1);

            if (bm1 != null) bm1.nextBucketID = b0.thisBucketID;
            b0.nextBucketID = b1.nextBucketID;
            if (b2 != null) b2.prevBucketID = b0.thisBucketID;
            if (b1.thisBucketID == lastBucketID) lastBucketID = b0.thisBucketID;

            Heap.Entry<Long, Long> b1entry = heapEntries.remove(b1.thisBucketID);
            if (b1entry != null) mergeCounts.delete(b1entry);
            updateMergeCountFor(bm1, b0);
            updateMergeCountFor(b0, b2);

            if (bm1 != null) store.putBucket(streamID, bm1.thisBucketID, bm1);
            store.putBucket(streamID, b0.thisBucketID, b0);
            if (b2 != null) store.putBucket(streamID, b2.thisBucketID, b2);
        }

        Bucket lastBucket = lastBucketID == -1 ? null : store.getBucket(streamID, lastBucketID);
        if (lastBucket != null && lastBucket.cEnd - lastBucket.cStart + 1 < firstWindowLength) {
            // last bucket isn't yet full; insert new value into it
            lastBucket.cEnd = N-1;
            lastBucket.tEnd = ts;
            lastBucket.insertValue(ts, value);
            store.putBucket(streamID, lastBucketID, lastBucket);
        } else {
            // create new bucket holding the latest element
            long newBucketID = lastBucketID + 1;
            Bucket newBucket = new Bucket(lastBucketID, newBucketID, -1, ts, ts, N-1, N-1);
            newBucket.insertValue(ts, value);
            store.putBucket(streamID, newBucketID, newBucket);
            if (lastBucket != null) {
                lastBucket.nextBucketID = newBucketID;
                updateMergeCountFor(lastBucket, newBucket);
                store.putBucket(streamID, lastBucketID, lastBucket);
            }
            lastBucketID = newBucketID;
        }
    }
}
