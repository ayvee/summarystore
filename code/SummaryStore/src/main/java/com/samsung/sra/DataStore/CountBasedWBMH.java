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
     * Do count-based WBMH with the specified windowing scheme. If the optional
     * numWindowsToBuffer argument is specified and positive, buffer the newest
     * numWindows buckets in memory, and only flush them to disk and update the
     * windowing once they're all full.
     */
    public CountBasedWBMH(Windowing windowing, int numWindowsToBuffer) {
        this.windowing = windowing;

        numWindowsInBuffer = numWindowsToBuffer;
        if (numWindowsInBuffer <= 0) numWindowsInBuffer = 0;
        List<Long> windowLengths = windowing.getSizeOfFirstKWindows(numWindowsToBuffer);
        for(int i = 0; i < numWindowsInBuffer; i++) {
            int length = windowLengths.get(i).intValue();
            this.bufferedWindowLengths.add(length);
            bufferSize += length;
        }
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
        if (bufferSize > 0) flushBuffer(manager);
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
        if (logger.isDebugEnabled() && N % 1_000_000 == 0) {
            logger.debug("N = {}, mergeCounts.size = {}", N, mergeCounts.getSize());
        }

        assert buffer.size() < bufferSize;
        buffer.add(new AbstractMap.SimpleEntry<>(ts, value));
        if (buffer.size() == bufferSize) { // buffer is full, flush
            flushFullBuffer(streamManager);
        }
    }

    void flushBuffer(StreamManager manager) throws RocksDBException {
        if (buffer.size() == bufferSize) {
            flushFullBuffer(manager);
        } else { // append elements one by one
            for (Map.Entry<Long, Object> entry: buffer) {
                appendUnbuffered(manager, entry.getKey(), entry.getValue());
            }
            buffer.clear();
        }
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

    /** alternate flushFullBuffer code, not yet tested */
    private void flushFullBufferAlt(StreamManager streamManager) throws RocksDBException {
        assert bufferSize > 0 && buffer.size() == bufferSize;

        // create new buckets and insert them into RocksDB
        {
            long prevBucketID = lastBucketID;
            Bucket prevBucket = lastBucketID == -1 ? null : streamManager.getBucket(lastBucketID);
            int cStart = 0, cEnd;
            for (int bucketSize : bufferedWindowLengths) {
                cEnd = cStart + bucketSize - 1;
                long currBucketID = prevBucketID + 1;
                Bucket currBucket = streamManager.createEmptyBucket(prevBucketID, currBucketID, -1,
                        buffer.get(cStart).getKey(), buffer.get(cEnd).getKey(),
                        cStart, cEnd);
                for (int c = cStart; c <= cEnd; ++c) {
                    Map.Entry<Long, Object> tsValue = buffer.get(c);
                    streamManager.insertValueIntoBucket(currBucket, tsValue.getKey(), tsValue.getValue());
                }

                if (prevBucket != null) {
                    prevBucket.nextBucketID = currBucketID;
                    streamManager.putBucket(prevBucketID, prevBucket);
                }
                prevBucketID = currBucketID;
                prevBucket = currBucket;
                cStart += bucketSize;
            }
            assert prevBucket != null;
            streamManager.putBucket(prevBucketID, prevBucket);
        }

        // update merge counts for the new buckets
        {
            long currBucketID = lastBucketID == -1 ? 0 : lastBucketID;
            Bucket currBucket = streamManager.getBucket(currBucketID);
            while (currBucket.nextBucketID != -1) {
                Bucket nextBucket = streamManager.getBucket(currBucket.nextBucketID);
                updateMergeCountFor(currBucket, nextBucket, N + bufferSize);
                currBucket = nextBucket;
            }
        }

        // process all merges
        processMergesUntil(streamManager, N + bufferSize);

        // insert complete, advance counter
        N += bufferSize;
    }

    private void flushFullBuffer(StreamManager streamManager) throws RocksDBException {
        int processedItem = 0;
        long iTs;
        Object iValue;
        Map.Entry<Long, Object> entry;
        long lastBucketIDBeforeMerge = lastBucketID;
        long lastNBeforeMerge = N + 1;

        long headBucketID = 0;

        for(int i = numWindowsInBuffer - 1; i >= 0; i--) {
            Bucket lastBucket;
            if(i == (numWindowsInBuffer - 1)) {
                lastBucket = null;
                headBucketID = lastBucketIDBeforeMerge + 1;
            }
            else {
                lastBucket = streamManager.getBucket(lastBucketIDBeforeMerge);
            }

            Bucket[] bucketList = new Bucket[bufferedWindowLengths.get(i)];

            long newBucketID = lastBucketIDBeforeMerge + 1;
            entry = buffer.get(processedItem);
            iTs = entry.getKey();
            iValue = entry.getValue();
            ++N;
            Bucket newBucket = streamManager.createEmptyBucket(lastBucketIDBeforeMerge, newBucketID, -1, iTs, iTs, N-1, N-1);
            streamManager.insertValueIntoBucket(newBucket, iTs, iValue);
            processedItem++;

            bucketList[0] = newBucket;

            for(int j = 1; j < bufferedWindowLengths.get(i); j++) {
                entry = buffer.get(processedItem);
                iTs = entry.getKey();
                iValue = entry.getValue();
                ++N;
                Bucket tmpBucket = streamManager.createEmptyBucket(-1, -1, -1, iTs, iTs, N-1, N-1);
                streamManager.insertValueIntoBucket(tmpBucket, iTs, iValue);
                bucketList[j] = tmpBucket;
                processedItem++;
            }

            streamManager.mergeBuckets(bucketList);

            streamManager.putBucket(newBucketID, newBucket);

            if(lastBucket != null) {
                lastBucket.nextBucketID = newBucketID;
                streamManager.putBucket(lastBucketIDBeforeMerge, lastBucket);
            }
            lastBucketIDBeforeMerge = newBucketID;

        }

        if(lastBucketID >= 0) {
            long tmpLastBucketID = lastBucketID;
            Bucket lastBucket = streamManager.getBucket(lastBucketID);
            Bucket headBucket = streamManager.getBucket(headBucketID);

            if(lastBucket != null && headBucket != null){
                lastBucket.nextBucketID = headBucketID;
                headBucket.prevBucketID = lastBucketID;
                streamManager.putBucket(lastBucketID, lastBucket);
                streamManager.putBucket(headBucketID, headBucket);
                lastBucketID = lastBucketIDBeforeMerge;
            }

            processMergeQueueForBuf(streamManager, lastNBeforeMerge, tmpLastBucketID);
        } else {
            lastBucketID = lastBucketIDBeforeMerge;
        }

        // print out the list of windows
        long prevBucketID = lastBucketID;

        do{
            Bucket curBucket = streamManager.getBucket(prevBucketID);
            prevBucketID = curBucket.prevBucketID;
            //System.out.println(curBucket.count);
        } while(prevBucketID != -1);

        buffer.clear();
    }

    private void processMergeQueueForBuf(StreamManager streamManager, long curN, long headBucketID) throws RocksDBException {
        if(headBucketID == -1) {
            return;
        } else {
            long curBucketID = headBucketID;
            do{
                Bucket curBucket = streamManager.getBucket(curBucketID);
                if(curBucket.nextBucketID != -1) {
                    Bucket nextBucket = streamManager.getBucket(curBucket.nextBucketID);
                    updateMergeCountFor(curBucket, nextBucket, curN + bufferSize);
                }
                curBucketID = curBucket.nextBucketID;
            } while(curBucketID != -1);
        }

        processMergesUntil(streamManager, curN + bufferSize);
    }
}
