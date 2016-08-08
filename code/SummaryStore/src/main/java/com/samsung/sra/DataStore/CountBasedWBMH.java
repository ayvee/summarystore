package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teneighty.heap.FibonacciHeap;
import org.teneighty.heap.Heap;

import java.util.*;

/**
 * WBMH implementation with
 * 1. One amortized bucket merge operation per insert
 * 2. O(log(# buckets)) book-keeping overhead per insert (comes from a priority
 *    queue we maintain to identify when buckets need to be merged)
 */
public class CountBasedWBMH implements WindowingMechanism {
    private static Logger logger = LoggerFactory.getLogger(CountBasedWBMH.class);
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
    transient private HashMap<Long, Heap.Entry<Long, Long>> heapEntries = new HashMap<>();

    private final ArrayList<Map.Entry<Long, Object>> buffer = new ArrayList<>();
    private final ArrayList<Integer> bufferedWindowLengths = new ArrayList<>();
    private int bufferSize;
    private int numWindowsInBuffer;


    /**
     * Do count-based WBMH with the specified windowing scheme. Buffer up to
     * numValuesToBuffer elements in memory, deferring window merges until either
     * the buffer fills up or flush() is called
     */
    public CountBasedWBMH(Windowing windowing, int numValuesToBuffer) {
        this.windowing = windowing;

        // figure out size/shape of buffer
        for (int K = 1000; K <= numValuesToBuffer; K *= 10) {
            // iterative deepening search for large enough K so that first K windows cover N elements
            bufferedWindowLengths.clear();
            bufferSize = 0;
            numWindowsInBuffer = 0;
            List<Long> windowLengths = windowing.getSizeOfFirstKWindows(K);
            int N = numValuesToBuffer;
            for (long windowLengthLong: windowLengths) {
                if (N <= 0) break;
                int windowLength = (int)windowLengthLong;
                bufferedWindowLengths.add(windowLength);
                bufferSize += windowLength;
                ++numWindowsInBuffer;
                N -= windowLength;
            }
            if (N <= 0) { // we were able to cover all N elements
                break;
            }
        }
        logger.info("Buffer covers {} windows and {} values", numWindowsInBuffer, bufferSize);
    }

    public CountBasedWBMH(Windowing windowing) {
        this(windowing, 0);
    }

    @Override
    public void populateTransientFields() {
        heapEntries = new HashMap<>();
        for (Heap.Entry<Long, Long> entry: mergeCounts) {
            heapEntries.put(entry.getValue(), entry);
        }
    }

    @Override
    public void append(StreamManager streamManager, long ts, Object value) throws RocksDBException {
        if (numWindowsInBuffer == 0) {
            appendUnbuffered(streamManager, ts, value);
        } else {
            appendBuffered(streamManager, ts, value);
        }
    }


    @Override
    public void close(StreamManager manager) throws RocksDBException {
        if (bufferSize > 0) flush(manager);
    }

    public void appendUnbuffered(StreamManager streamManager, long ts, Object value) throws RocksDBException {
        if (logger.isDebugEnabled() && N % 1_000_000 == 0) {
            logger.debug("N = {}, mergeCounts.size = {}", N, mergeCounts.getSize());
        }

        // merge existing buckets
        processMergesUntil(streamManager, N + 1);

        // insert newest element, creating a new bucket for it if necessary
        Bucket lastBucket = lastBucketID == -1 ? null : streamManager.getBucket(lastBucketID);
        if (lastBucket != null && lastBucket.cEnd - lastBucket.cStart + 1 < windowing.getSizeOfFirstWindow()) {
            // last bucket isn't yet full; insert new value into it
            lastBucket.cEnd = N;
            lastBucket.tEnd = ts;
            streamManager.insertValueIntoBucket(lastBucket, ts, value);
            streamManager.putBucket(lastBucketID, lastBucket);
        } else {
            // create new bucket holding the latest element
            long newBucketID = lastBucketID + 1;
            Bucket newBucket = streamManager.createEmptyBucket(lastBucketID, newBucketID, -1, ts, ts, N, N);
            streamManager.insertValueIntoBucket(newBucket, ts, value);
            streamManager.putBucket(newBucketID, newBucket);
            if (lastBucket != null) {
                lastBucket.nextBucketID = newBucketID;
                updateMergeCountFor(lastBucket, newBucket, N + 1);
                streamManager.putBucket(lastBucketID, lastBucket);
            }
            lastBucketID = newBucketID;
        }

        ++N;
    }

    public void appendBuffered(StreamManager streamManager, long ts, Object value) throws RocksDBException{
        assert buffer.size() < bufferSize;
        buffer.add(new AbstractMap.SimpleEntry<>(ts, value));
        if (buffer.size() == bufferSize) { // buffer is full, flush
            flushFullBuffer(streamManager);
            buffer.clear();
        }
    }

    @Override
    public void flush(StreamManager manager) throws RocksDBException {
        if (numWindowsInBuffer == 0) return;
        if (buffer.size() == bufferSize) {
            flushFullBuffer(manager);
        } else { // append elements one by one
            for (Map.Entry<Long, Object> entry: buffer) {
                appendUnbuffered(manager, entry.getKey(), entry.getValue());
            }
        }
        buffer.clear();
    }

    /**
     * Set mergeCounts[(b1, b2)] = first N' >= N such that (b0, b1) will need to be
     *                             merged after N' elements have been inserted
     */
    private void updateMergeCountFor(Bucket b0, Bucket b1, long N) {
        if (b0 == null || b1 == null) return;

        Heap.Entry<Long, Long> existingEntry = heapEntries.remove(b0.thisBucketID);
        if (existingEntry != null) mergeCounts.delete(existingEntry);

        long newMergeCount = windowing.getFirstContainingTime(b0.cStart, b1.cEnd, N);
        if (newMergeCount != -1) {
            heapEntries.put(b0.thisBucketID, mergeCounts.insert(newMergeCount, b0.thisBucketID));
        }
    }


    /** Advance count marker to N, apply the WBMH test, process any merges that result */
    private void processMergesUntil(StreamManager streamManager, long N) throws RocksDBException {
        while (!mergeCounts.isEmpty() && mergeCounts.getMinimum().getKey() <= N) {
            Heap.Entry<Long, Long> entry = mergeCounts.extractMinimum();
            Heap.Entry<Long, Long> removed = heapEntries.remove(entry.getValue());
            assert removed == entry;
            Bucket b0 = streamManager.getBucket(entry.getValue());
            // We will now merge b0's successor b1 into b0. We also need to update b{-1}'s and
            // b2's prev and next pointers and b{-1} and b0's heap entries
            assert b0.nextBucketID != -1;
            Bucket b1 = streamManager.getBucket(b0.nextBucketID, true); // note: this deletes b1 from bucketStore
            Bucket b2 = b1.nextBucketID == -1 ? null : streamManager.getBucket(b1.nextBucketID);
            Bucket bm1 = b0.prevBucketID == -1 ? null : streamManager.getBucket(b0.prevBucketID); // b{-1}

            streamManager.mergeBuckets(b0, b1);

            if (bm1 != null) bm1.nextBucketID = b0.thisBucketID;
            b0.nextBucketID = b1.nextBucketID;
            if (b2 != null) b2.prevBucketID = b0.thisBucketID;
            if (b1.thisBucketID == lastBucketID) lastBucketID = b0.thisBucketID;

            Heap.Entry<Long, Long> b1entry = heapEntries.remove(b1.thisBucketID);
            if (b1entry != null) mergeCounts.delete(b1entry);
            updateMergeCountFor(bm1, b0, N);
            updateMergeCountFor(b0, b2, N);

            if (bm1 != null) streamManager.putBucket(bm1.thisBucketID, bm1);
            streamManager.putBucket(b0.thisBucketID, b0);
            if (b2 != null) streamManager.putBucket(b2.thisBucketID, b2);
        }
    }

    private void flushFullBuffer(StreamManager streamManager) throws RocksDBException {
        assert bufferSize > 0 && buffer.size() == bufferSize;

        long lastExtantBucketID = lastBucketID;
        Bucket lastExtantBucket;
        if (lastExtantBucketID != -1) {
            lastExtantBucket = streamManager.getBucket(lastExtantBucketID);
            lastExtantBucket.nextBucketID = lastExtantBucketID + 1;
        } else {
            lastExtantBucket = null;
        }
        Bucket[] newBuckets = new Bucket[numWindowsInBuffer];

        // create new buckets
        {
            long cBase = (lastExtantBucket == null) ? 0 : lastExtantBucket.cEnd + 1;
            int cStartOffset = 0, cEndOffset;
            for (int bucketNum = 0; bucketNum < numWindowsInBuffer; ++bucketNum) {
                int bucketSize = bufferedWindowLengths.get(numWindowsInBuffer - 1 - bucketNum);
                cEndOffset = cStartOffset + bucketSize - 1;
                Bucket bucket = streamManager.createEmptyBucket(
                        lastExtantBucketID + bucketNum,
                        lastExtantBucketID + bucketNum + 1,
                        ((bucketNum == numWindowsInBuffer - 1) ? -1 : lastExtantBucketID + bucketNum + 2),
                        buffer.get(cStartOffset).getKey(), buffer.get(cEndOffset).getKey(),
                        cBase + cStartOffset, cBase + cEndOffset);
                for (int c = cStartOffset; c <= cEndOffset; ++c) {
                    Map.Entry<Long, Object> tsValue = buffer.get(c);
                    streamManager.insertValueIntoBucket(bucket, tsValue.getKey(), tsValue.getValue());
                }
                newBuckets[bucketNum] = bucket;

                cStartOffset += bucketSize;
            }
        }

        // update merge counts for the new buckets
        if (lastExtantBucket != null) updateMergeCountFor(lastExtantBucket, newBuckets[0], N + bufferSize);
        for (int b = 0; b < numWindowsInBuffer-1; ++b) {
            updateMergeCountFor(newBuckets[b], newBuckets[b+1], N + bufferSize);
        }

        // insert all modified buckets into RocksDB
        if (lastExtantBucket != null) streamManager.putBucket(lastExtantBucketID, lastExtantBucket);
        for (int b = 0; b < numWindowsInBuffer; ++b) {
            assert newBuckets[b].thisBucketID == lastExtantBucketID + b + 1;
            streamManager.putBucket(newBuckets[b].thisBucketID, newBuckets[b]);
        }

        // insert complete, advance counter
        N += bufferSize;
        lastBucketID = lastExtantBucketID + numWindowsInBuffer;

        // process any pending merges (should all be in the older buckets)
        processMergesUntil(streamManager, N);
    }
}
