package com.samsung.sra.DataStore;

import org.nustaq.serialization.FSTConfiguration;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements the time-decay and landmark parts of SummaryStore. API:
 *    register(streamID, aggregateDataStructure)
 *    append(streamID, Collection<FlaggedValue>)
 *    query(streamID, t1, t2, aggregateFunction)
 * At this point we make no distinction between time and count, i.e. we assume
 * exactly one element arrives at t = 0, 1, 2, 3, ...
 * Created by a.vulimiri on 1/15/16.
 */
public class TimeDecayedStore implements DataStore {
    private final Logger logger = Logger.getLogger(TimeDecayedStore.class.getName());
    private BucketMerger merger;
    private RocksDB rocksDB = null;
    private Options rocksDBOptions = null;

    /**
     * The buckets proper are stored in RocksDB. We maintain additional in-memory indexes and
     * metadata in StreamInfo to help reads and writes. The append operation keeps StreamInfo
     * consistent with the base data on RocksDB. */
    private final HashMap<StreamID, StreamInfo> streamsInfo;

    /** We will persist streamsInfo in RocksDB, storing it under this special key. Note that this key is
     * 1 byte, as opposed to the 8 byte keys we use for buckets, so it won't interfere with bucket storage
     */
    private final static byte[] streamInfoSpecialKey = {0};

    private void persistStreamsInfo() throws RocksDBException {
        rocksDB.put(streamInfoSpecialKey, fstConf.asByteArray(streamsInfo));
    }

    public TimeDecayedStore(String rocksDBPath, BucketMerger merger) throws RocksDBException {
        rocksDBOptions = new Options().setCreateIfMissing(true);
        rocksDB = RocksDB.open(rocksDBOptions, rocksDBPath);
        this.merger = merger;

        byte[] streamsInfoBytes = rocksDB.get(streamInfoSpecialKey);
        if (streamsInfoBytes != null) {
            streamsInfo = (HashMap<StreamID, StreamInfo>)fstConf.asObject(streamsInfoBytes);
        } else {
            streamsInfo = new HashMap<StreamID, StreamInfo>();
        }
    }

    // FST is a fast serialization library, used to quickly convert Buckets to/from RocksDB byte arrays
    private static final FSTConfiguration fstConf;
    static {
        fstConf = FSTConfiguration.createDefaultConfiguration();
        fstConf.registerClass(Bucket.class);
        fstConf.registerClass(StreamInfo.class);

        RocksDB.loadLibrary();
    }

    public void registerStream(final StreamID streamID) throws StreamException, RocksDBException {
        // TODO: also register what data structure we will use for each bucket
        synchronized (streamsInfo) {
            if (streamsInfo.containsKey(streamID)) {
                throw new StreamException("attempting to register streamID " + streamID + " multiple times");
            } else {
                streamsInfo.put(streamID, new StreamInfo(streamID));
                persistStreamsInfo();
            }
        }
    }

    public Object query(StreamID streamID, int queryType, int t0, int t1) throws StreamException, QueryException, RocksDBException {
        if (t0 > t1 || t0 < 0) {
            throw new QueryException("[" + t0 + ", " + t1 + "] is not a valid time interval");
        }
        final StreamInfo streamInfo;
        synchronized (streamsInfo) {
            if (!streamsInfo.containsKey(streamID)) {
                throw new StreamException("attempting to read from unregistered stream " + streamID);
            } else {
                streamInfo = streamsInfo.get(streamID);
            }
        }
        synchronized (streamInfo.readerSyncObj) {
            if (t1 >= streamInfo.numElements) {
                throw new QueryException("[" + t0 + ", " + t1 + "] is not a valid time interval");
            }
            TreeMap<Integer, BucketID> index = streamInfo.timeIndex;
            Integer l = index.floorKey(t0); // first startN <= t0
            Integer r = index.higherKey(t1); // first startN > t1
            if (r == null) {
                r = streamInfo.numElements;
            }
            // Query on all buckets with l <= startN < r.  FIXME: this overapproximates in some cases with landmarks
            SortedMap<Integer, BucketID> spanningBucketsIDs = index.subMap(l, true, r, false);
            List<Bucket> spanningBuckets = new ArrayList<Bucket>();
            // TODO: RocksDB multiget
            for (BucketID bucketID: spanningBucketsIDs.values()) {
                spanningBuckets.add(rocksGet(streamID, bucketID));
            }
            return Bucket.multiBucketQuery(spanningBuckets, queryType, t0, t1);
        }
    }

    public void append(StreamID streamID, Collection<FlaggedValue> values) throws StreamException, LandmarkEventException, RocksDBException {
        if (values == null || values.isEmpty()) {
            return;
        }
        final StreamInfo streamInfo;
        synchronized (streamsInfo) {
            if (!streamsInfo.containsKey(streamID)) {
                throw new StreamException("attempting to append to unregistered stream " + streamID);
            } else {
                streamInfo = streamsInfo.get(streamID);
            }
        }

        synchronized (streamInfo.writerSyncObj) {
            /* All writes will be serialized at this point. Reads are still allowed. We will lock
             the reader object below once we've done the bucket merge math and are ready to start
             modifying the data structure */
            int N0 = streamInfo.numElements, N = N0 + values.size();
            // id of last bucket currently present in RocksDB
            BucketID lastBucketID = null;
            if (streamInfo.numElements > 0) {
                for (BucketID bucketID: streamInfo.buckets.keySet()) {
                    lastBucketID = bucketID;
                }
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

            // insert values into buckets, creating new buckets as necessary
            Map<BucketID, TreeMap<Integer, Object>> pendingInserts =
                    processInserts(values, N0, streamInfo, baseBuckets, landmarkBuckets, lastBucketID);

            // figure out which base buckets need to be merged
            assert mergeInputIsSane(baseBuckets, N0, N);
            List<List<BucketID>> pendingMerges;
            {
                LinkedHashMap<BucketID, BucketInfo> baseBucketsCopy = new LinkedHashMap<BucketID, BucketInfo>();
                for (Map.Entry<BucketID, BucketInfo> entry : baseBuckets.entrySet()) {
                    baseBucketsCopy.put(entry.getKey(), new BucketInfo(entry.getValue()));
                }
                pendingMerges = merger.merge(baseBucketsCopy, N0, N);
            }
            assert mergeOutputIsSane(pendingMerges, baseBuckets, N);

            // compile pendingInserts and pendingMerges into a list of actions on each bucket
            TreeMap<BucketID, BucketModification> pendingModifications =
                    identifyBucketModifications(pendingInserts, pendingMerges, baseBuckets, landmarkBuckets, lastBucketID);

            // process the modifications
            for (BucketModification mod: pendingModifications.values()) {
                // block readers and modify the data structure
                synchronized (streamInfo.readerSyncObj) {
                    processBucketModification(mod, streamInfo);
                }
            }
        }
    }

    /**
     * Figure out which bucket every new <time, value> pair needs to be inserted into,
     * creating new base or landmark buckets as necessary. Modifies baseBuckets and landmarkBuckets */
    private Map<BucketID, TreeMap<Integer, Object>> processInserts(
            Collection<FlaggedValue> values, int N0,
            StreamInfo streamInfo,
            LinkedHashMap<BucketID, BucketInfo> baseBuckets, LinkedHashMap<BucketID, BucketInfo> landmarkBuckets,
            BucketID lastBucketID) throws LandmarkEventException {
        Map<BucketID, TreeMap<Integer, Object>> pendingInserts = new HashMap<BucketID, TreeMap<Integer, Object>>();
        BucketID activeLandmarkBucket = streamInfo.activeLandmarkBucket;
        BucketID nextBucketID = lastBucketID != null ? lastBucketID.nextBucketID() : new BucketID(0);
        int n = N0 - 1;
        for (FlaggedValue fv: values) {
            ++n;
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
        streamInfo.activeLandmarkBucket = activeLandmarkBucket;
        return pendingInserts;
    }

    /**
     * Combine pendingInserts and pendingMerges into a list of bucket modifications
     */
    private TreeMap<BucketID, BucketModification> identifyBucketModifications(
            Map<BucketID, TreeMap<Integer, Object>> pendingInserts, List<List<BucketID>> pendingMerges,
            LinkedHashMap<BucketID, BucketInfo> baseBuckets, LinkedHashMap<BucketID, BucketInfo> landmarkBuckets,
            BucketID lastBucketID) {
        TreeMap<BucketID, BucketModification> pendingModifications = new TreeMap<BucketID, BucketModification>();
        // first process base buckets
        for (List<BucketID> mergeList: pendingMerges) {
            if (mergeList == null || mergeList.isEmpty()) {
                logger.log(Level.WARNING, "empty or null list in merge output");
            } else if (mergeList.size() == 1) { // no merge, just insert
                BucketID bucketID = mergeList.get(0);
                boolean isANewBucket = (lastBucketID == null || bucketID.compareTo(lastBucketID) > 0);
                TreeMap<Integer, Object> inserts = pendingInserts.get(bucketID);
                if (inserts != null || isANewBucket) {
                    BucketModification mod = new BucketModification(bucketID, isANewBucket, new BucketInfo(baseBuckets.get(bucketID)));
                    mod.valuesToInsert = inserts;
                    pendingModifications.put(bucketID, mod);
                } // else this is an existing bucket that is being written back unmodified, i.e. a no-op

            } else { // merge
                BucketID mergeTarget = null;
                TreeMap<Integer, Object> inserts = null;
                List<BucketID> merges = new ArrayList<BucketID>();
                int endN = -1;
                for (BucketID bucketID: mergeList) {
                    if (mergeTarget == null) {
                        // mergeList = mergeTarget : mergees
                        mergeTarget = bucketID;
                        // only base buckets should be merged
                        assert baseBuckets.containsKey(mergeTarget) && !landmarkBuckets.containsKey(mergeTarget);
                        inserts = pendingInserts.get(mergeTarget);
                        if (inserts == null) {
                            inserts = new TreeMap<Integer, Object>();
                        }
                        continue;
                    }
                    // only base buckets should be merged
                    assert baseBuckets.containsKey(bucketID) && !landmarkBuckets.containsKey(bucketID);
                    BucketInfo info = baseBuckets.get(bucketID);
                    assert info.endN > endN;
                    endN = info.endN;
                    if (lastBucketID != null && lastBucketID.compareTo(bucketID) >= 0) {
                        // this is not a virtual merge: mergee actually exists in RocksDB right now
                        merges.add(bucketID);
                    }
                    TreeMap<Integer, Object> intermediateInserts = pendingInserts.get(bucketID);
                    if (intermediateInserts != null) {
                        inserts.putAll(intermediateInserts);
                    }
                }
                if (!merges.isEmpty() || !inserts.isEmpty()) {
                    boolean isANewBucket = (lastBucketID == null || lastBucketID.compareTo(mergeTarget) < 0);
                    BucketInfo finalBucketInfo = new BucketInfo(baseBuckets.get(mergeTarget));
                    finalBucketInfo.endN = endN;
                    BucketModification mod = new BucketModification(mergeTarget, isANewBucket, finalBucketInfo);
                    if (!merges.isEmpty()) {
                        mod.bucketsToMergeInto = merges;
                    }
                    if (!inserts.isEmpty()) {
                        mod.valuesToInsert = inserts;
                    }
                    pendingModifications.put(mergeTarget, mod);
                }
            }
        }
        // next process landmark buckets
        for (BucketID bucketID: landmarkBuckets.keySet()) {
            TreeMap<Integer, Object> inserts = pendingInserts.get(bucketID);
            if (inserts != null) {
                boolean isANewBucket = (lastBucketID == null || lastBucketID.compareTo(bucketID) < 0);
                BucketModification mod = new BucketModification(bucketID, isANewBucket, new BucketInfo(landmarkBuckets.get(bucketID)));
                mod.valuesToInsert = inserts;
                assert inserts.lastKey() == mod.finalBucketInfo.endN; // landmark buckets should always be "full"
                pendingModifications.put(bucketID, mod);
            }
        }
        return pendingModifications;
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

        public BucketModification(BucketID bucketID, boolean isANewBucket, BucketInfo finalBucketInfo) {
            this.bucketID = bucketID;
            this.isANewBucket = isANewBucket;
            this.finalBucketInfo = finalBucketInfo;
        }

        @Override
        public String toString() {
            String ret = "BucketModification(" + bucketID + "):";
            if (bucketsToMergeInto != null) {
                ret += " merge[ ";
                for (BucketID mergee: bucketsToMergeInto) {
                    ret += mergee + " ";
                }
                ret += "]";
            }
            if (valuesToInsert != null) {
                ret += " inserts[ ";
                for (Map.Entry<Integer, Object> entry: valuesToInsert.entrySet()) {
                    ret += "(" + entry.getKey() + ", " + entry.getValue() + ") ";
                }
                ret += "]";
            }
            if (isANewBucket) {
                ret += " create";
            }
            ret += " final state " + finalBucketInfo;
            return ret;
        }
    }

    /**
     * This is the basic atomic modify operation in the system.
     */
    private void processBucketModification(BucketModification mod, StreamInfo streamInfo) throws RocksDBException {
        List<Bucket> mergees = new ArrayList<Bucket>();
        if (mod.bucketsToMergeInto != null && !mod.bucketsToMergeInto.isEmpty()) {
            for (BucketID mergee: mod.bucketsToMergeInto) {
                mergees.add(rocksGet(streamInfo.streamID, mergee, true));
                streamInfo.buckets.remove(mergee);
            }
        }
        Bucket target;
        if (mod.isANewBucket) {
            target = new Bucket(mod.finalBucketInfo);
        } else {
            target = rocksGet(streamInfo.streamID, mod.bucketID);
        }
        target.merge(mergees, mod.valuesToInsert, mod.finalBucketInfo.endN);
        assert mod.finalBucketInfo.startN == target.info.startN && mod.finalBucketInfo.endN == target.info.endN;
        rocksPut(streamInfo.streamID, mod.bucketID, target);

        streamInfo.buckets.put(mod.bucketID, mod.finalBucketInfo);
        streamInfo.numElements = Math.max(streamInfo.numElements, 1 + mod.finalBucketInfo.endN);

        streamInfo.reconstructTimeIndex();

        synchronized (streamsInfo) {
            persistStreamsInfo();
        }
    }

    /**
     * RocksDB key = <streamID, bucketID>. Since we ensure bucketIDs are assigned in increasing
     * order of startN, this lays out data in temporal order within streams
     */
    private byte[] getRocksDBKey(StreamID streamID, BucketID bucketID) {
        ByteBuffer bytebuf = ByteBuffer.allocate(StreamID.byteCount + BucketID.byteCount);
        streamID.writeToByteBuffer(bytebuf);
        bucketID.writeToByteBuffer(bytebuf);
        bytebuf.flip();
        return bytebuf.array();
    }

    private Bucket rocksGet(StreamID streamID, BucketID bucketID) throws RocksDBException {
        return rocksGet(streamID, bucketID, false);
    }

    private Bucket rocksGet(StreamID streamID, BucketID bucketID, boolean delete) throws RocksDBException {
        byte[] rocksKey = getRocksDBKey(streamID, bucketID);
        byte[] rocksValue = rocksDB.get(rocksKey);
        if (delete) {
            rocksDB.remove(rocksKey);
        }
        return (Bucket)fstConf.asObject(rocksValue);
    }

    private void rocksPut(StreamID streamID, BucketID bucketID, Bucket bucket) throws RocksDBException {
        byte[] rocksKey = getRocksDBKey(streamID, bucketID);
        byte[] rocksValue = fstConf.asByteArray(bucket);
        rocksDB.put(rocksKey, rocksValue);
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
        if (prevEnd != N - 1) {
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
                prevEnd = bucketInfo.endN;
            }
        }
        if (prevEnd != N - 1) {
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
            String storeLoc = "/tmp/tdstore";
            // FIXME: add a deleteStream/resetDatabase operation
            Runtime.getRuntime().exec(new String[]{"rm", "-rf", storeLoc});
            store = new TimeDecayedStore(storeLoc, new WBMHBucketMerger(3));
            StreamID streamID = new StreamID(0);
            store.registerStream(streamID);
            List<FlaggedValue> values = new ArrayList<FlaggedValue>();
            for (int i = 0; i < 10; ++i) {
                values.add(new FlaggedValue(i+1));
                if (i == 4) values.get(i).landmarkStartsHere = true;
                if (i == 6) values.get(i).landmarkEndsHere = true;
            }
            store.append(streamID, values);
            store.printBucketState(streamID);
            int t0 = 0, t1 = 9;
            System.out.println(
                    "sum[" + t0 + ", " + t1 + "] = " + store.query(streamID, Bucket.QUERY_SUM, t0, t1) + "; " +
                    "count[" + t0 + ", " + t1 + "] = " + store.query(streamID, Bucket.QUERY_COUNT, t0, t1));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (store != null) {
                store.close();
            }
        }
    }
}