package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teneighty.heap.FibonacciHeap;
import org.teneighty.heap.Heap;

import java.util.HashMap;

/**
 * WBMH implementation with
 * 1. Two amortized bucket merge operations per insert
 * 2. O(log(# buckets)) book-keeping overhead per insert (comes from a priority
 *    queue we maintain to identify when buckets need to be merged)
 */
public class CountBasedWBMH implements WindowingMechanism {
    private static Logger logger = LoggerFactory.getLogger(CountBasedWBMH.class);
    private final long streamID;
    private final Windowing windowing;

    private long lastBucketID = -1;
    private long N = 0;

    /* Priority queue, mapping each BucketID b_i to the time at which b_{i+1} will be
     * merged into it. Using a Fibonacci heap instead of the Java Collections PriorityQueue
     * because we need an efficient arbitrary-element delete.
     *
     * Why this particular Fibonacci heap implementation?
     * https://gabormakrai.wordpress.com/2015/02/11/experimenting-with-dijkstras-algorithm/
     */
    private final FibonacciHeap<Long, Long> mergeCounts = new FibonacciHeap<>();
    private final HashMap<Long, Heap.Entry<Long, Long>> heapEntries = new HashMap<>();


    public CountBasedWBMH(long streamID, Windowing windowing) {
        this.streamID = streamID;
        this.windowing = windowing;
    }


    /**
     * Update the priority queue's entry for the merge pair (b0, b1)
     * */
    private void updateMergeCountFor(Bucket b0, Bucket b1) {
        if (b0 == null || b1 == null) return;

        Heap.Entry<Long, Long> existingEntry = heapEntries.remove(b0.thisBucketID);
        if (existingEntry != null) mergeCounts.delete(existingEntry);

        long newMergeCount = windowing.getFirstContainingTime(b0.cStart, b1.cEnd, N);
        if (newMergeCount != -1) {
            heapEntries.put(b0.thisBucketID, mergeCounts.insert(newMergeCount, b0.thisBucketID));
        }
    }

    @Override
    public void append(SummaryStore store, long ts, Object value) throws RocksDBException {
        if (logger.isDebugEnabled() && N % 1_000_000 == 0) {
            logger.debug("N = {}, mergeCounts.size = {}", N, mergeCounts.getSize());
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
        if (lastBucket != null && lastBucket.cEnd - lastBucket.cStart + 1 < windowing.getSizeOfFirstWindow()) {
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
