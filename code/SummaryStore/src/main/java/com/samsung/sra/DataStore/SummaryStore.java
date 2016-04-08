package com.samsung.sra.DataStore;

import org.nustaq.serialization.FSTConfiguration;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Time-decayed aggregate storage
 */
public class SummaryStore implements DataStore {
    private static final Level logLevel = Level.INFO;
    private static final Logger logger = Logger.getLogger(SummaryStore.class.getName());
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
        final Object readLock = new Object(), writeLock = new Object();
        // How many values have we inserted so far?
        long numValues = 0;
        // What was the timestamp of the latest value appended?
        Timestamp lastValueTimestamp = null;
        // FIXME: Implicit assumption here that time starts at 0 in every stream; we can discuss if that should change

        // TODO: register an object to track what data structure we will use for each bucket

        /** Index mapping bucket.tStart -> bucketID, used to answer queries */
        final TreeMap<Timestamp, BucketID> temporalIndex = new TreeMap<>();

        StreamInfo(StreamID streamID) {
            this.streamID = streamID;
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
                new HashMap<>();
    }

    // FST is a fast serialization library, used to quickly convert Buckets to/from RocksDB byte arrays
    private static final FSTConfiguration fstConf;
    static {
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(logLevel);
        logger.addHandler(handler);
        logger.setLevel(logLevel);

        fstConf = FSTConfiguration.createDefaultConfiguration();
        fstConf.registerClass(Bucket.class);
        fstConf.registerClass(StreamInfo.class);
        fstConf.registerClass(CountBasedWBMH.class);
        fstConf.registerClass(SlowCountBasedWBMH.class);

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
        synchronized (streamInfo.readLock) {
            if (t1.compareTo(streamInfo.lastValueTimestamp) > 0) {
                throw new QueryException("[" + t0 + ", " + t1 + "] is not a valid time interval");
            }
            TreeMap<Timestamp, BucketID> index = streamInfo.temporalIndex;
            Timestamp l = index.floorKey(t0); // first bucket with tStart <= t0
            Timestamp r = index.higherKey(t1); // first bucket with tStart > t1
            //logger.log(Level.FINEST, "Overapproximated time range = [" + l + ", " + r + ")");
            if (r == null) {
                r = index.lastKey();
            }
            // Query on all buckets with l <= tStart < r
            SortedMap<Timestamp, BucketID> spanningBucketsIDs = index.subMap(l, true, r, false);
            Bucket first = null;
            List<Bucket> rest = new ArrayList<>();
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

    public void append(StreamID streamID, Timestamp ts, Object value) throws StreamException, RocksDBException {
        final StreamInfo streamInfo;
        synchronized (streamsInfo) {
            if (!streamsInfo.containsKey(streamID)) {
                throw new StreamException("attempting to append to unregistered stream " + streamID);
            } else {
                streamInfo = streamsInfo.get(streamID);
            }
        }

        synchronized (streamInfo.writeLock) {
            /* All writes will be serialized at this point. Reads are still allowed. We will lock
             readers below once we've done the bucket merge math and are ready to start modifying
             the data structure */

            // ask windowing mechanism which existing buckets to merge and/or which new buckets
            // to create, in response to adding this value
            List<BucketModification> bucketMods = windowingMechanism.computeModifications(ts, value);

            // we've done the bucket modification math: now lock all readers and process bucket changes
            synchronized (streamInfo.readLock) {
                // 1. Update set of buckets
                for (BucketModification mod : bucketMods) {
                    //logger.log(Level.FINEST, "Executing bucket modification " + mod);
                    mod.process(this, streamInfo);
                }
                // 2. Insert the new value into the appropriate bucket
                processInsert(streamInfo, ts, value);

                /* FIXME
                synchronized (streamsInfo) {
                    persistStreamsInfo();
                }*/
            }
        }
    }

    interface BucketModification {
        /**
         * Process modifications to the store. This function is responsible for updating
         * the buckets in RocksDB as well as the in-memory indexes in StreamInfo (but not
         * persisting the indexes to disk)
         */
        void process(SummaryStore store, StreamInfo streamInfo) throws RocksDBException;
    }

    /**
     * Merge one or more successor buckets into the mergee bucket
     */
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
            List<Bucket> sources = new ArrayList<>();
            for (BucketID srcID: merges) {
                Bucket src = store.rocksGet(streamInfo.streamID, srcID);
                sources.add(src);
                streamInfo.temporalIndex.remove(src.tStart);
            }
            target.merge(sources);
            store.rocksPut(streamInfo.streamID, mergee, target);
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

    /**
     * Create an empty bucket with the specified metadata
     */
    static class BucketCreateModification implements BucketModification {
        private final BucketID bucketID;
        private final Timestamp tStart;
        private final long cStart;

        BucketCreateModification(BucketID bucketID, Timestamp bucketTStart, long cStart) {
            this.bucketID = bucketID;
            this.tStart = bucketTStart;
            this.cStart = cStart;
        }

        @Override
        public void process(SummaryStore store, StreamInfo streamInfo) throws RocksDBException {
            Bucket bucket = new Bucket(bucketID, tStart, cStart);
            store.rocksPut(streamInfo.streamID, bucketID, bucket);

            streamInfo.temporalIndex.put(tStart, bucketID);
        }

        @Override
        public String toString() {
            return "Create bucket " + bucketID + " with tStart = " + tStart + ", cStart = " + cStart;
        }
    }

    private void processInsert(StreamInfo streamInfo, Timestamp ts, Object value) throws RocksDBException {
        BucketID destinationID = streamInfo.temporalIndex.floorEntry(ts).getValue();
        assert destinationID != null;
        Bucket bucket = rocksGet(streamInfo.streamID, destinationID);
        bucket.insertValue(ts, value);
        //logger.log(Level.FINEST, "Inserted value <" + ts + ", " + value + "> into bucket " + bucket);
        rocksPut(streamInfo.streamID, destinationID, bucket);
        streamInfo.numValues += 1;
        streamInfo.lastValueTimestamp = ts;
    }

    /**
     * RocksDB key = <streamID, bucketID>. Since we ensure bucketIDs are assigned in increasing
     * order, this lays out data in temporal order within streams
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
        for (BucketID bucketID: streamInfo.temporalIndex.values()) {
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
            ret += si.temporalIndex.size() * (StreamID.byteCount + BucketID.byteCount + Bucket.byteCount);
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
            for (long i = 0; i < 10; ++i) {
                store.append(streamID, new Timestamp(i), i + 1);
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
        /*long N = 65536;
        for (int W = 1; W <= 65536; W *= 2) {
            ExponentialWindowLengths.getWindowingOfSize(N, W);
        }*/
    }
}