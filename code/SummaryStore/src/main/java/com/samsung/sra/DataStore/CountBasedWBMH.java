package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teneighty.heap.FibonacciHeap;
import org.teneighty.heap.Heap;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;

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
    private long bufSize = 1023;
    private int appendBufCnt = 0;
    private int totalBucketInBuf = 4;

    /* Priority queue, mapping each BucketID b_i to the time at which b_{i+1} will be
     * merged into it. Using a Fibonacci heap instead of the Java Collections PriorityQueue
     * because we need an efficient arbitrary-element delete.
     *
     * Why this particular Fibonacci heap implementation?
     * https://gabormakrai.wordpress.com/2015/02/11/experimenting-with-dijkstras-algorithm/
     */
    private final FibonacciHeap<Long, Long> mergeCounts = new FibonacciHeap<>();
    private final HashMap<Long, Heap.Entry<Long, Long>> heapEntries = new HashMap<>();

    private final ArrayList<Map.Entry<Long, Object>> ingestBuf = new ArrayList<Map.Entry<Long, Object>>();
    private final ArrayList<Integer> bucketListInBuf = new ArrayList<Integer>();

    public CountBasedWBMH(Windowing windowing) {
        this.windowing = windowing;

        this.bufSize = windowing.getTotalWindowLength(totalBucketInBuf);

	for(int i = 0; i < totalBucketInBuf; i++) {
	    this.bucketListInBuf.add((int)windowing.getWindowLength(i));
        }	

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

    private void updateMergeCountForBatch(Bucket b0, Bucket b1, long newN) {
        if (b0 == null || b1 == null) return;

        Heap.Entry<Long, Long> existingEntry = heapEntries.remove(b0.thisBucketID);
        if (existingEntry != null) mergeCounts.delete(existingEntry);

        long newMergeCount = windowing.getFirstContainingTime(b0.cStart, b1.cEnd, newN);
        if (newMergeCount != -1) {
            heapEntries.put(b0.thisBucketID, mergeCounts.insert(newMergeCount, b0.thisBucketID));
        }
    }


    private void processMergeQueue(StreamManager streamManager, long curN, long numBuckets) throws RocksDBException {

	for(int i = 0; i < numBuckets; i++) {
            while (!mergeCounts.isEmpty() && mergeCounts.getMinimum().getKey() <= (curN+i)) {
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
                updateMergeCountFor(bm1, b0);
                updateMergeCountFor(b0, b2);

                if (bm1 != null) streamManager.putBucket(bm1.thisBucketID, bm1);
                streamManager.putBucket(b0.thisBucketID, b0);
                if (b2 != null) streamManager.putBucket(b2.thisBucketID, b2);
            }

	    if(lastBucketID == -1) {
		return;
	    } 
	    else {
		Bucket lastBucket = streamManager.getBucket(lastBucketID);
		if(lastBucket.prevBucketID != -1) {
		    Bucket preBucket = streamManager.getBucket(lastBucket.prevBucketID);
		    updateMergeCountFor(preBucket, lastBucket);
		}
            }
	}
    }    



    private void processMergeQueueForBuf(StreamManager streamManager, long curN, long headBucketID) throws RocksDBException {

	    if(headBucketID == -1) {
		return;
	    } 
	    else {

		long curBucketID = headBucketID;
		do{
		    Bucket curBucket = streamManager.getBucket(curBucketID);
		    if(curBucket.nextBucketID != -1) {
		        Bucket nextBucket = streamManager.getBucket(curBucket.nextBucketID);
		        updateMergeCountForBatch(curBucket, nextBucket, curN + bufSize);
			curBucketID = curBucket.nextBucketID;
		    }
		    else{ 
			break;
		    }
		} while(curBucketID != -1);

            }


            while (!mergeCounts.isEmpty() && mergeCounts.getMinimum().getKey() <= (curN+bufSize)) {
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
                updateMergeCountForBatch(bm1, b0, curN + bufSize);
                updateMergeCountForBatch(b0, b2, curN + bufSize);

                if (bm1 != null) streamManager.putBucket(bm1.thisBucketID, bm1);
                streamManager.putBucket(b0.thisBucketID, b0);
                if (b2 != null) streamManager.putBucket(b2.thisBucketID, b2);
            }

    }    


    @Override
    public void append(StreamManager streamManager, long ts, Object value) throws RocksDBException {
        if (logger.isDebugEnabled() && N % 1_000_000 == 0) {
            logger.debug("N = {}, mergeCounts.size = {}", N, mergeCounts.getSize());
        }
        ++N;

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
            updateMergeCountFor(bm1, b0);
            updateMergeCountFor(b0, b2);

            if (bm1 != null) streamManager.putBucket(bm1.thisBucketID, bm1);
            streamManager.putBucket(b0.thisBucketID, b0);
            if (b2 != null) streamManager.putBucket(b2.thisBucketID, b2);
        }

        Bucket lastBucket = lastBucketID == -1 ? null : streamManager.getBucket(lastBucketID);
        if (lastBucket != null && lastBucket.cEnd - lastBucket.cStart + 1 < windowing.getSizeOfFirstWindow()) {
            // last bucket isn't yet full; insert new value into it
            lastBucket.cEnd = N-1;
            lastBucket.tEnd = ts;
	    streamManager.insertValueIntoBucket(lastBucket, ts, value);
            streamManager.putBucket(lastBucketID, lastBucket);
        } else {
            // create new bucket holding the latest element
            long newBucketID = lastBucketID + 1;
            Bucket newBucket = streamManager.createEmptyBucket(lastBucketID, newBucketID, -1, ts, ts, N-1, N-1);
	    streamManager.insertValueIntoBucket(newBucket, ts, value);
            streamManager.putBucket(newBucketID, newBucket);
            if (lastBucket != null) {
                lastBucket.nextBucketID = newBucketID;
                updateMergeCountFor(lastBucket, newBucket);
                streamManager.putBucket(lastBucketID, lastBucket);
            }
            lastBucketID = newBucketID;
        }
    }


    @Override
    public void appendBuf(StreamManager streamManager, long ts, Object value) throws RocksDBException{
        if (logger.isDebugEnabled() && N % 1_000_000 == 0) {
            logger.debug("N = {}, mergeCounts.size = {}", N, mergeCounts.getSize());
        }

        if (appendBufCnt < bufSize) {
	    ingestBuf.add(appendBufCnt, new AbstractMap.SimpleEntry(ts, value));
            appendBufCnt++;
	    return;
        } 
        else {
	    /**
	     * ingest buffer is full, let's do the merge
	     */
	    appendBufCnt = 0;
	    int processedItem = 0;
	    long iTs;
	    Object iValue;
	    Map.Entry<Long, Object> entry;
	    long lastBucketIDBeforeMerge = lastBucketID;
	    long lastNBeforeMerge;

	    lastNBeforeMerge = N+1;

	    long headBucketID = 0;

	    for(int i = totalBucketInBuf - 1; i >= 0; i--) {
		Bucket lastBucket = null;
		if(i == (totalBucketInBuf - 1)) {
		    lastBucket = null;
		    headBucketID = lastBucketIDBeforeMerge + 1;
		}
		else {
		    lastBucket = streamManager.getBucket(lastBucketIDBeforeMerge);
		}

		Bucket[] bucketList = new Bucket[bucketListInBuf.get(i)];

                long newBucketID = lastBucketIDBeforeMerge + 1;
	        entry = ingestBuf.get(processedItem);
		iTs = entry.getKey();
		iValue = entry.getValue();
		++N;
	        Bucket newBucket = streamManager.createEmptyBucket(lastBucketIDBeforeMerge, newBucketID, -1, iTs, iTs, N-1, N-1); 
		streamManager.insertValueIntoBucket(newBucket, iTs, iValue); 
		processedItem++;

		bucketList[0] = newBucket;

	        for(int j = 1; j < bucketListInBuf.get(i); j++) {
		    entry = ingestBuf.get(processedItem);
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
		/**
		  * call this for one by one merge
		  */
		//processMergeQueue(streamManager, lastNBeforeMerge, bufSize);
	
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

		/**
		  * call this for batch merge
		  */
	        processMergeQueueForBuf(streamManager, lastNBeforeMerge, tmpLastBucketID);

            }
	    else{
		lastBucketID = lastBucketIDBeforeMerge;
	    }

	    // print out the list of windows
            long prevBucketID = lastBucketID;

            do{
		Bucket curBucket = streamManager.getBucket(prevBucketID);
		prevBucketID = curBucket.prevBucketID; 	
		//System.out.println(curBucket.count);	
	
	    } while(prevBucketID != -1);

            /**
	     * done the merge
	     */
	    ingestBuf.add(appendBufCnt, new AbstractMap.SimpleEntry(ts, value));
            appendBufCnt++;
	    return;
        }	

    }
}
