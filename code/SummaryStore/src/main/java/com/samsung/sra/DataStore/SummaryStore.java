package com.samsung.sra.DataStore;

import org.nustaq.serialization.FSTConfiguration;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements the time-decay and landmark parts of SummaryStore. API:
 *    register(streamID, aggregateDataStructure)
 *    append(streamID, value)
 *    query(streamID, t1, t2, aggregateFunction)
 */
public class SummaryStore implements DataStore {
    private final Logger logger = Logger.getLogger(SummaryStore.class.getName());
    private WindowingMechanism windowingMechanism;
    private RocksDB rocksDB = null;
    private Options rocksDBOptions = null;

    /**
     * The buckets proper are stored in RocksDB. We maintain additional in-memory indexes and
     * metadata in StreamInfo to help reads and writes. The append operation keeps StreamInfo
     * consistent with the base data on RocksDB. */
    private static class StreamInfo implements Serializable {
        final StreamID streamID;
        // Object that readers and writers respectively will use "synchronized" with (acts as a mutex)
        final Object readerSyncObj = new Object(), writerSyncObj = new Object();
        // How many values have we inserted so far?
        int numValues = 0;
        // What was the timestamp of the latest value appended?
        Timestamp lastValueTimestamp = null;
        // FIXME: Implicit assumption here that time starts at 0 in every stream; we can discuss if that should change

        // TODO: register an object to track what data structure we will use for each bucket

        /** All buckets for this stream */
        final LinkedHashMap<BucketID, BucketMetadata> buckets = new LinkedHashMap<BucketID, BucketMetadata>();
        /** If there is an active (unclosed) landmark bucket, its ID */
        BucketID activeLandmarkBucket = null;
        /** Index mapping bucket.tStart -> bucketID, used to answer queries */
        final TreeMap<Timestamp, BucketID> timeIndex = new TreeMap<Timestamp, BucketID>();

        StreamInfo(StreamID streamID) {
            this.streamID = streamID;
        }

        void reconstructTimeIndex() {
            timeIndex.clear();
            for (BucketMetadata bucketMetadata : buckets.values()) {
                assert bucketMetadata.tStart != null;
                timeIndex.put(bucketMetadata.tStart, bucketMetadata.bucketID);
            }
        }
    }

    private final HashMap<StreamID, StreamInfo> streamsInfo;

    /** We will persist streamsInfo in RocksDB, storing it under this special key. Note that this key is
     * 1 byte, as opposed to the 8 byte keys we use for buckets, so it won't interfere with bucket storage
     */
    private final static byte[] streamInfoSpecialKey = {0};

    private void persistStreamsInfo() throws RocksDBException {
        rocksDB.put(streamInfoSpecialKey, fstConf.asByteArray(streamsInfo));
    }

    public SummaryStore(String rocksDBPath, WindowingMechanism windowingMechanism) throws RocksDBException {
        rocksDBOptions = new Options().setCreateIfMissing(true);
        rocksDB = RocksDB.open(rocksDBOptions, rocksDBPath);
        this.windowingMechanism = windowingMechanism;

        byte[] streamsInfoBytes = rocksDB.get(streamInfoSpecialKey);
        streamsInfo = streamsInfoBytes != null ?
                (HashMap<StreamID, StreamInfo>)fstConf.asObject(streamsInfoBytes) :
                new HashMap<StreamID, StreamInfo>();
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

    public Object query(StreamID streamID, Timestamp t0, Timestamp t1, QueryType queryType, Object[] queryParams) throws StreamException, QueryException, RocksDBException {
        if (t0.compareTo(t1) > 0 || t0.compareTo(new Timestamp(0)) < 0) {
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
            if (t1.compareTo(streamInfo.lastValueTimestamp) > 0) {
                throw new QueryException("[" + t0 + ", " + t1 + "] is not a valid time interval");
            }
            TreeMap<Timestamp, BucketID> index = streamInfo.timeIndex;
            Timestamp l = index.floorKey(t0); // first bucket with tStart <= t0
            Timestamp r = index.higherKey(t1); // first bucket with tStart > t1
            if (r == null) {
                r = index.lastKey();
            }
            // Query on all buckets with l <= tStart < r.  FIXME: this overapproximates in some cases with landmarks
            SortedMap<Timestamp, BucketID> spanningBucketsIDs = index.subMap(l, true, r, false);
            Bucket first = null;
            List<Bucket> rest = new ArrayList<Bucket>();
            // TODO: RocksDB multiget
            for (BucketID bucketID: spanningBucketsIDs.values()) {
                Bucket bucket = rocksGet(streamID, bucketID);
                if (first == null) {
                    first = bucket;
                } else {
                    rest.add(bucket);
                }
            }
            assert first != null;
            return first.multiQuery(rest, t0, t1, queryType, queryParams);
        }
    }

    public void append(StreamID streamID, Timestamp ts, Object value, boolean landmarkStartsHere, boolean landmarkEndsHere)
            throws StreamException, LandmarkEventException, RocksDBException {
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
            boolean isLandmarkValue = streamInfo.activeLandmarkBucket != null || landmarkStartsHere;
            if (streamInfo.activeLandmarkBucket != null && landmarkStartsHere) {
                throw new LandmarkEventException();
            }

            // ask windowing mechanism which existing buckets to merge and/or which new buckets
            // to create, in response to adding this value
            List<BucketModification> bucketMods = windowingMechanism.computeModifications(
                    streamInfo.buckets, streamInfo.numValues, streamInfo.lastValueTimestamp,
                    ts, value, isLandmarkValue);

            // we've done the bucket modification math: now lock all readers and process bucket changes
            synchronized (streamInfo.readerSyncObj) {
                // 1. Update set of buckets
                for (BucketModification mod : bucketMods) {
                    logger.log(Level.FINEST, "Executing bucket modification " + mod);
                    mod.process(this, streamInfo);
                }
                // 2. Insert the new value into the appropriate bucket
                processInsert(streamInfo, ts, value, isLandmarkValue);
            }
        }
    }

    interface BucketModification {
        /**
         * Process modifications to the store. This function is responsible for updating
         * the buckets in RocksDB as well as the indexes in StreamInfo
         */
        void process(SummaryStore store, StreamInfo streamInfo) throws RocksDBException;
    }

    static class BucketMergeModification implements BucketModification {
        private final BucketID mergee;
        private final List<BucketID> merges;

        BucketMergeModification(BucketID mergee, List<BucketID> merges) {
            this.mergee = mergee;
            this.merges = merges;
        }

        @Override
        public void process(SummaryStore store, StreamInfo streamInfo) throws RocksDBException {
            if (merges == null || merges.isEmpty()) {
                return;
            }
            Bucket target = store.rocksGet(streamInfo.streamID, mergee);
            List<Bucket> sources = new ArrayList<Bucket>();
            for (BucketID srcID: merges) {
                sources.add(store.rocksGet(streamInfo.streamID, srcID));
                streamInfo.buckets.remove(srcID);
            }
            target.merge(sources);
            store.rocksPut(streamInfo.streamID, mergee, target);
            streamInfo.buckets.put(mergee, target.metadata);

            streamInfo.reconstructTimeIndex();

            synchronized (store.streamsInfo) {
                store.persistStreamsInfo();
            }
        }

        @Override
        public String toString() {
            String ret = "BucketMerge<" + mergee;
            for (BucketID merged: merges) {
                ret += ", " + merged;
            }
            ret += ">";
            return ret;
        }
    }

    static class BucketCreateModification implements BucketModification {
        private final BucketMetadata metadata;

        BucketCreateModification(BucketMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public void process(SummaryStore store, StreamInfo streamInfo) throws RocksDBException {
            Bucket bucket = new Bucket(metadata);
            store.rocksPut(streamInfo.streamID, metadata.bucketID, bucket);
            streamInfo.buckets.put(metadata.bucketID, metadata);

            streamInfo.reconstructTimeIndex();

            synchronized (store.streamsInfo) {
                store.persistStreamsInfo();
            }
        }

        @Override
        public String toString() {
            return "BucketCreate" + metadata;
        }
    }

    private void processInsert(StreamInfo streamInfo, Timestamp ts, Object value, boolean isLandmarkValue) throws RocksDBException {
        BucketID destinationID;
        if (isLandmarkValue) {
            if (streamInfo.activeLandmarkBucket == null) {
                BucketID idOfLastExtantBucket = null;
                for (BucketID id: streamInfo.buckets.keySet()) {
                    idOfLastExtantBucket = id;
                }
                assert idOfLastExtantBucket != null;
                streamInfo.activeLandmarkBucket = idOfLastExtantBucket.nextBucketID();
                // FIXME: we're supposed to use an enumerating bucket for landmarks
                Bucket landmarkBucket = new Bucket(new BucketMetadata(
                        streamInfo.activeLandmarkBucket, ts, streamInfo.numValues, true));
                rocksPut(streamInfo.streamID, streamInfo.activeLandmarkBucket, landmarkBucket);
                streamInfo.buckets.put(streamInfo.activeLandmarkBucket, landmarkBucket.metadata);
            }
            destinationID = streamInfo.activeLandmarkBucket;
        } else {
            destinationID = null;
            for (BucketMetadata md: streamInfo.buckets.values()) {
                if (!md.isLandmark) {
                    destinationID = md.bucketID;
                }
            }
            assert destinationID != null;
        }
        Bucket bucket = rocksGet(streamInfo.streamID, destinationID);
        bucket.insertValue(ts, value);
        logger.log(Level.FINEST, "Inserted value <" + ts + ", " + value + "> into bucket " + bucket.metadata);
        rocksPut(streamInfo.streamID, destinationID, bucket);
        streamInfo.numValues += 1;
        streamInfo.lastValueTimestamp = ts;
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

    private void printBucketState(StreamID streamID) throws RocksDBException {
        StreamInfo streamInfo = streamsInfo.get(streamID);
        System.out.println("Stream " + streamID + " with " + streamInfo.numValues + " elements:");
        for (BucketID bucketID: streamInfo.buckets.keySet()) {
            System.out.println("\t" + rocksGet(streamID, bucketID));
        }
    }

    public void close() {
        // FIXME: should wait for any processing appends to terminate first
        if (rocksDB != null) {
            rocksDB.close();
        }
        rocksDBOptions.dispose();
    }

    public long getStoreSizeInBytes() {
        // TODO: lock all writers
        long ret = 0;
        for (StreamInfo si: streamsInfo.values()) {
            // FIXME: this does not account for the size of the time/count markers tracked by the index
            ret += si.buckets.size() * (StreamID.byteCount + BucketID.byteCount + Bucket.byteCount);
        }
        return ret;
    }

    public static void main(String[] args) {
        SummaryStore store = null;
        try {
            String storeLoc = "/tmp/tdstore";
            // FIXME: add a deleteStream/resetDatabase operation
            Runtime.getRuntime().exec(new String[]{"rm", "-rf", storeLoc}).waitFor();
            //store = new SummaryStore(storeLoc, new ExponentialWBMHWindowingMechanism(3));
            store = new SummaryStore(storeLoc, new CountBasedWBMH(new ExponentialWindowLengths(2)));
            StreamID streamID = new StreamID(0);
            store.registerStream(streamID);
            for (int i = 0; i < 10; ++i) {
                boolean landmarkStartsHere = false, landmarkEndsHere = false;
                if (i == 4) landmarkStartsHere = true;
                if (i == 6) landmarkEndsHere = true;
                store.append(streamID, new Timestamp(i), i + 1, landmarkStartsHere, landmarkEndsHere);
                store.printBucketState(streamID);
            }
            Timestamp t0 = new Timestamp(0), t1 = new Timestamp(4);
            System.out.println(
                    "sum[" + t0 + ", " + t1 + "] = " + store.query(streamID, t0, t1, QueryType.SUM, null) + "; " +
                    "count[" + t0 + ", " + t1 + "] = " + store.query(streamID, t0, t1, QueryType.COUNT, null));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (store != null) {
                store.close();
            }
        }
    }
}