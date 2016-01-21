package com.samsung.sra.TimeDecayedStore;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements the time-decay and landmark parts of SummaryStore. API:
 *    register(streamID, aggregateDataStructure)
 *    append(streamID, List<FlaggedValue>)
 *    query(streamID, t1, t2, aggregateFunction)
 * At this point we make no distinction between time and count, i.e. we assume
 * exactly one element arrives at t = 0, 1, 2, 3, ...
 * Created by a.vulimiri on 1/15/16.
 */
public class TimeDecayedStore {
    private final Logger logger = Logger.getLogger(TimeDecayedStore.class.getName());
    private final String rocksDBPath;
    private BucketMerger merger;
    private RocksDB rocksDB = null;
    private Options rocksDBOptions = null;

    /* The buckets proper are stored in RocksDB. We maintain additional in-memory indexes
    to help reads and writes:
        streamsInfo: basic stream metadata + list of bucket endpoints for each stream
        activeLandmarkBuckets: BucketIDs for streams that have an open landmark bucket
     Right now, the append operation is responsible for ensuring these indexes are synchronized
     with the base data on RocksDB.
     */
    private final Map<StreamID, StreamInfo> streamsInfo;
    private Map<StreamID, BucketID> activeLandmarkBuckets;

    public TimeDecayedStore(String rocksDBPath, BucketMerger merger) throws RocksDBException {
        /* TODO: implement a lock to ensure exclusive access to this RocksDB path.
                 RocksDB does not seem to have built-in locking */
        this.rocksDBPath = rocksDBPath;
        RocksDB.loadLibrary();
        rocksDBOptions = new Options().setCreateIfMissing(true);
        rocksDB = RocksDB.open(rocksDBOptions, rocksDBPath);
        this.merger = merger;

        this.streamsInfo = new HashMap<StreamID, StreamInfo>();
        this.activeLandmarkBuckets = new HashMap<StreamID, BucketID>();
    }

    public void registerStream(final StreamID streamID) throws StreamException {
        // TODO: also register what data structure we will use for each bucket
        synchronized (streamsInfo) {
            if (streamsInfo.containsKey(streamID)) {
                throw new StreamException("attempting to register streamID " + streamID + " multiple times");
            } else {
                streamsInfo.put(streamID, new StreamInfo(streamID));
                activeLandmarkBuckets.put(streamID, null);
            }
        }
    }

    public void append(StreamID streamID, List<FlaggedValue> values) throws StreamException, LandmarkEventException {
        if (values == null || values.isEmpty()) {
            return;
        }
        final StreamInfo streamInfo;
        synchronized (streamsInfo) {
            if (!streamsInfo.containsKey(streamID)) {
                throw new StreamException("attempting to append to unregistered streamID " + streamID);
            } else {
                streamInfo = streamsInfo.get(streamID);
            }
        }

        synchronized (streamInfo.writerSyncObj) {
            /* All writes will be serialized at this point. Reads are still allowed. We will lock
             the reader object below once we've done the bucket merge math and are ready to start
             modifying the data structure */
            int N0 = streamInfo.numElements, N = N0 + values.size();
            // id of last bucket inserted so far and next bucket to insert
            BucketID bucketID0 = null, nextBucketID;
            if (streamInfo.numElements > 0) {
                for (BucketID bucketID: streamInfo.buckets.keySet()) {
                    bucketID0 = bucketID;
                }
                nextBucketID = bucketID0.nextBucketID();
            } else {
                nextBucketID = new BucketID(0);
            }

            LinkedHashMap<BucketID, BucketInfo>
                    baseBuckets = new LinkedHashMap<BucketID, BucketInfo>(),
                    landmarkBuckets = new LinkedHashMap<BucketID, BucketInfo>();
            for (BucketInfo bucketInfo: streamInfo.buckets.values()) {
                BucketInfo bucketInfoCopy = new BucketInfo(bucketInfo);
                if (bucketInfo.isLandmark) {
                    landmarkBuckets.put(bucketInfo.bucketID, bucketInfoCopy);
                } else {
                    baseBuckets.put(bucketInfo.bucketID, bucketInfoCopy);
                }
            }
            BucketID activeLandmarkBucket = activeLandmarkBuckets.get(streamID);

            /* Process inserts: figure out which bucket every new <time, value> pair needs to be inserted into,
            creating new base or landmark buckets as necessary */
            Map<BucketID, TreeMap<Integer, Object>> pendingInserts = new HashMap<BucketID, TreeMap<Integer, Object>>();
            for (int i = 0; i < values.size(); ++i) {
                int n = N0 + i;
                FlaggedValue fv = values.get(i);
                /* We always create a new base bucket of size 1 for every inserted element. As with any other
                 base bucket, it can be empty if the value at that position goes into a landmark bucket instead */
                BucketID newBaseBucket = nextBucketID;
                baseBuckets.put(newBaseBucket, new BucketInfo(newBaseBucket, n, n, false));
                nextBucketID = nextBucketID.nextBucketID();

                if (fv.landmarkStartsHere) {
                    // create a landmark bucket
                    if (activeLandmarkBucket != null) {
                        throw new LandmarkEventException();
                    }
                    activeLandmarkBucket = nextBucketID;
                    landmarkBuckets.put(activeLandmarkBucket, new BucketInfo(activeLandmarkBucket, n, n, true));
                    nextBucketID = nextBucketID.nextBucketID();
                }
                if (activeLandmarkBucket != null) {
                    // insert into active landmark bucket
                    assert landmarkBuckets.containsKey(activeLandmarkBucket);
                    landmarkBuckets.get(activeLandmarkBucket).endN = n;
                    if (!pendingInserts.containsKey(activeLandmarkBucket)) {
                        pendingInserts.put(activeLandmarkBucket, new TreeMap<Integer, Object>());
                    }
                    pendingInserts.get(activeLandmarkBucket).put(n, fv.value);
                } else {
                    // insert into the new base bucket of size 1
                    if (!pendingInserts.containsKey(newBaseBucket)) {
                        pendingInserts.put(newBaseBucket, new TreeMap<Integer, Object>());
                    }
                    pendingInserts.get(newBaseBucket).put(n, fv.value);
                }
                if (fv.landmarkEndsHere) {
                    /* Close the landmark bucket. Right now we don't track open/closed in the bucket explicitly,
                    so all we need to do is unmark activeLandmarkBucket */
                    if (activeLandmarkBucket == null) {
                        throw new LandmarkEventException();
                    }
                    activeLandmarkBucket = null;
                }
            }

            // Figure out which base buckets need to be merged
            assert mergeInputIsSane(baseBuckets, N0, N);
            LinkedHashMap<BucketID, BucketInfo> baseBucketsCopy = new LinkedHashMap<BucketID, BucketInfo>();
            for (Map.Entry<BucketID, BucketInfo> entry: baseBuckets.entrySet()) {
                baseBucketsCopy.put(entry.getKey(), new BucketInfo(entry.getValue()));
            }
            List<List<BucketID>> pendingMerges = merger.merge(baseBucketsCopy, N0, N);
            assert mergeOutputIsSane(pendingMerges, baseBucketsCopy, N);

            // Compile pendingInserts and pendingMerges into a list of actions on each bucket
            /* TreeMap sorts modifications on BucketID. This is where the assumption that
            BucketIDs are assigned in increasing order becomes significant */
            TreeMap<BucketID, BucketModification> pendingModifications = new TreeMap<BucketID, BucketModification>();
            // first process base buckets
            for (List<BucketID> mergeList: pendingMerges) {
                if (mergeList == null || mergeList.isEmpty()) {
                    logger.log(Level.WARNING, "empty or null list in merge output");
                } else if (mergeList.size() == 1) { // no merge
                    BucketID bucketID = mergeList.get(0);
                    TreeMap<Integer, Object> inserts = pendingInserts.get(bucketID);
                    if (inserts != null) {
                        boolean isANewBucket = (bucketID0 == null || bucketID.compareTo(bucketID0) > 0);
                        BucketModification mod = new BucketModification(bucketID, isANewBucket, false);
                        mod.valuesToInsert = inserts;
                        mod.finalBucketInfo.startN = baseBuckets.get(bucketID).startN;
                        mod.finalBucketInfo.endN = inserts.lastKey();
                        pendingModifications.put(bucketID, mod);
                    }
                } else { // merge
                    BucketID targetBucketID = null;
                    List<BucketID> merges = new ArrayList<BucketID>();
                    TreeMap<Integer, Object> inserts = new TreeMap<Integer, Object>();
                    for (BucketID bucketID: mergeList) {
                        if (targetBucketID == null) {
                            targetBucketID = bucketID;
                            continue;
                        }
                        if (bucketID0 != null && bucketID.compareTo(bucketID0) <= 0) { // this bucket actually exists right now
                            merges.add(bucketID);
                        }
                        TreeMap<Integer, Object> intermediateInserts = pendingInserts.get(bucketID);
                        if (intermediateInserts != null) {
                            inserts.putAll(intermediateInserts);
                        }
                    }
                    if (!merges.isEmpty() || !inserts.isEmpty()) {
                        boolean isANewBucket = (bucketID0 == null || targetBucketID.compareTo(bucketID0) > 0);
                        BucketModification mod = new BucketModification(targetBucketID, isANewBucket, false);
                        mod.finalBucketInfo.startN = baseBuckets.get(targetBucketID).startN;
                        if (!merges.isEmpty()) {
                            mod.bucketsToMergeInto = merges;
                            mod.finalBucketInfo.endN = baseBuckets.get(merges.get(merges.size() - 1)).endN;
                        }
                        if (!inserts.isEmpty()) {
                            mod.valuesToInsert = inserts;
                            mod.finalBucketInfo.endN = inserts.lastKey();
                        }
                        pendingModifications.put(targetBucketID, mod);
                    }
                }
            }
            // next process landmark buckets
            for (BucketID bucketID: landmarkBuckets.keySet()) {
                TreeMap<Integer, Object> inserts = pendingInserts.get(bucketID);
                if (inserts != null) {
                    boolean isANewBucket = (bucketID0 == null || bucketID.compareTo(bucketID0) > 0);
                    BucketModification mod = new BucketModification(bucketID, isANewBucket, true);
                    mod.valuesToInsert = inserts;
                    mod.finalBucketInfo.startN = landmarkBuckets.get(bucketID).startN;
                    mod.finalBucketInfo.endN = inserts.lastKey();
                    pendingModifications.put(bucketID, mod);
                }
            }

            // Process the modifications
            for (BucketModification mod: pendingModifications.values()) {
                // block readers and modify the data structure
                synchronized (streamInfo.readerSyncObj) {
                    processBucketModification(mod, streamInfo);
                }
            }
        }
    }

    /**
     * Merge zero or more successor buckets into the IDed bucket, then append zero or more
     * more values at the end. Merged buckets must be contiguous, and inserted values must be
     * sorted on time, newest value at the end. We do not sanity check that these requirements
     * are met.
     */
    private static class BucketModification {
        public final BucketID bucketID;
        public List<BucketID> bucketsToMergeInto = null;
        public TreeMap<Integer, Object> valuesToInsert = null;
        public final boolean isANewBucket; // bucket does not yet exist in RocksDB, needs to be created
        public final BucketInfo finalBucketInfo;

        public BucketModification(BucketID bucketID, boolean isANewBucket, boolean isLandmark) {
            this.bucketID = bucketID;
            this.isANewBucket = isANewBucket;
            this.finalBucketInfo = new BucketInfo(bucketID, isLandmark);
        }
    }

    // This is the basic atomic modify operation in the system
    private void processBucketModification(BucketModification mod, StreamInfo streamInfo) {
        // TODO: actually modify the buckets in RocksDB
        // merge buckets
        if (mod.bucketsToMergeInto != null && !mod.bucketsToMergeInto.isEmpty()) {
            for (BucketID mergee: mod.bucketsToMergeInto) {
                streamInfo.buckets.remove(mergee);
            }
        }
        // insert elements

        streamInfo.buckets.put(mod.bucketID, mod.finalBucketInfo);
        streamInfo.numElements = Math.max(streamInfo.numElements, 1 + mod.finalBucketInfo.endN);
    }

    private void printBucketState(StreamID streamID) {
        StreamInfo streamInfo = streamsInfo.get(streamID);
        System.out.println("Stream " + streamID + ": " + streamInfo.numElements + " elements");
        for (BucketInfo bucketInfo: streamInfo.buckets.values()) {
            System.out.println(
                    "\t size " + (bucketInfo.endN - bucketInfo.startN + 1) +
                    " span [" + bucketInfo.startN + ", " + bucketInfo.endN + "] " +
                    "bucket " + bucketInfo.bucketID + " " +
                    (bucketInfo.isLandmark ? "landmark" : "non-landmark"));
        }
    }

    private boolean mergeInputIsSane(LinkedHashMap<BucketID, BucketInfo> baseBuckets, int N0, int N) {
        if (baseBuckets == null) {
            logger.log(Level.SEVERE, "Problem in merge input: null baseBuckets");
            return false;
        }
        if (N0 >= N) {
            logger.log(Level.SEVERE, "Problem in merge input: N0 >= N (" + N0 + " >= " + N + ")");
            return false;
        }
        int prevEnd = -1;
        for (BucketInfo bucketInfo: baseBuckets.values()) {
            if (bucketInfo.startN != prevEnd + 1) {
                logger.log(Level.SEVERE, "Problem in merge input: bucket gap, bucket " + bucketInfo.bucketID
                        + " starts at " + bucketInfo.startN + " but previous bucket ends at " + prevEnd);
                return false;
            }
            prevEnd = bucketInfo.endN;
        }
        if (prevEnd != N) {
            logger.log(Level.SEVERE, "Problem in merge input: invalid N, last bucket ends at "
                    + prevEnd + " but N = " + N);
            return false;
        }
        return true;
    }

    private boolean mergeOutputIsSane(List<List<BucketID>> pendingMerges, LinkedHashMap<BucketID, BucketInfo> baseBuckets, int N) {
        if (pendingMerges == null) {
            logger.log(Level.SEVERE, "Problem in merge output: null pendingMerges");
            return false;
        }
        int prevEnd = -1;
        for (List<BucketID> blist: pendingMerges) {
            for (BucketID bucketID: blist) {
                BucketInfo bucketInfo = baseBuckets.get(bucketID);
                if (bucketInfo.startN != prevEnd + 1) {
                    logger.log(Level.SEVERE, "Problem in merge output: bucket gap, bucket " + bucketInfo.bucketID
                            + " starts at " + bucketInfo.startN + " but previous bucket ends at " + prevEnd);
                    return false;
                }
            }
        }
        if (prevEnd != N) {
            logger.log(Level.SEVERE, "Problem in merge output: invalid N, last bucket ends at "
                    + prevEnd + " but N = " + N);
            return false;
        }
        return true;
    }

    public void close() {
        // FIXME: should wait for any processing appends to terminate first
        if (rocksDB != null) {
            rocksDB.close();
        }
        rocksDBOptions.dispose();
    }

    public static void main(String[] args) {
        TimeDecayedStore store = null;
        try {
            store = new TimeDecayedStore("/tmp/tdstore", new WBMHBucketMerger(3));
            StreamID streamID = new StreamID(0);
            store.registerStream(streamID);
            store.printBucketState(streamID);
            for (int i = 0; i < 10; ++i) {
                List<FlaggedValue> values = new ArrayList<FlaggedValue>();
                values.add(new FlaggedValue(i));
                store.append(streamID, values);
                store.printBucketState(streamID);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (store != null) {
                store.close();
            }
        }
    }
}