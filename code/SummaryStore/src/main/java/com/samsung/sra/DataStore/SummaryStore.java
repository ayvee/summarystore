package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Time-decayed aggregate storage
 */
public class SummaryStore implements DataStore {
    private static final Level logLevel = Level.INFO;
    private static final Logger logger = Logger.getLogger(SummaryStore.class.getName());

    private final BucketStore bucketStore;

    /**
     * The buckets proper are stored in bucketStore. We maintain additional in-memory indexes and
     * metadata in StreamInfo to help reads and writes. The append operation keeps StreamInfo
     * consistent with the base data in bucketStore. */
     static class StreamInfo implements Serializable {
        final StreamID streamID;
        // How many values have we inserted so far?
        long numValues = 0;
        // What was the timestamp of the latest value appended?
        Timestamp lastValueTimestamp = null;

        final ReadWriteLock lock = new ReentrantReadWriteLock();

        /* Read index, maps bucket.tStart -> bucketID. Used to answer queries */
        final TreeMap<Timestamp, BucketID> temporalIndex = new TreeMap<>();

        /* WindowingMechanism object. Maintains write indexes internally, which will be serialized
         * along with the rest of StreamInfo when persistStreamsInfo() is called */
        final WindowingMechanism windowingMechanism;

        StreamInfo(StreamID streamID, WindowingMechanism windowingMechanism) {
            this.streamID = streamID;
            this.windowingMechanism = windowingMechanism;
        }
    }

    private final HashMap<StreamID, StreamInfo> streamsInfo;

    private void persistStreamsInfo() throws RocksDBException {
        bucketStore.putIndexes(streamsInfo);
    }

    public SummaryStore(BucketStore bucketStore) throws RocksDBException {
        this.bucketStore = bucketStore;
        Object uncast = bucketStore.getIndexes();
        streamsInfo = uncast != null ?
                (HashMap<StreamID, StreamInfo>)uncast :
                new HashMap<>();
    }

    static {
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(logLevel);
        logger.addHandler(handler);
        logger.setLevel(logLevel);
    }

    public void registerStream(final StreamID streamID, WindowingMechanism windowingMechanism) throws StreamException, RocksDBException {
        // TODO: also register what data structure we will use for each bucket
        synchronized (streamsInfo) {
            if (streamsInfo.containsKey(streamID)) {
                throw new StreamException("attempting to register streamID " + streamID + " multiple times");
            } else {
                streamsInfo.put(streamID, new StreamInfo(streamID, windowingMechanism));
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
        streamInfo.lock.readLock().lock();
        try {
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
                Bucket bucket = bucketStore.getBucket(streamID, bucketID);
                if (first == null) {
                    first = bucket;
                } else {
                    rest.add(bucket);
                }
            }
            assert first != null;
            return first.multiQuery(rest, t0, t1, queryType, queryParams);
        } finally {
            streamInfo.lock.readLock().unlock();
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

        streamInfo.lock.writeLock().lock();
        try {
            /* All writes will be serialized at this point. Reads are still allowed. We will lock
             readers below once we've done the bucket merge math and are ready to start modifying
             the data structure */

            // ask windowing mechanism which existing buckets to merge and/or which new buckets
            // to create, in response to adding this value
            List<BucketModification> bucketMods = streamInfo.windowingMechanism.computeModifications(ts, value);

            // we've done the bucket modification math: now lock all readers and process bucket changes
            streamInfo.lock.readLock().lock();
            try {
                // 1. Update set of buckets
                for (BucketModification mod : bucketMods) {
                    //logger.log(Level.FINEST, "Executing bucket modification " + mod);
                    mod.process(this, streamInfo);
                }
                // 2. Insert the new value into the appropriate bucket
                processInsert(streamInfo, ts, value);
            } finally {
                streamInfo.lock.readLock().unlock();
            }
        } finally {
            streamInfo.lock.writeLock().unlock();
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
            Bucket target = store.bucketStore.getBucket(streamInfo.streamID, mergee);
            List<Bucket> sources = new ArrayList<>();
            for (BucketID srcID: merges) {
                Bucket src = store.bucketStore.getBucket(streamInfo.streamID, srcID, true);
                sources.add(src);
                streamInfo.temporalIndex.remove(src.tStart);
            }
            target.merge(sources);
            store.bucketStore.putBucket(streamInfo.streamID, mergee, target);
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
            store.bucketStore.putBucket(streamInfo.streamID, bucketID, bucket);

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
        Bucket bucket = bucketStore.getBucket(streamInfo.streamID, destinationID);
        bucket.insertValue(ts, value);
        //logger.log(Level.FINEST, "Inserted value <" + ts + ", " + value + "> into bucket " + bucket);
        bucketStore.putBucket(streamInfo.streamID, destinationID, bucket);
        streamInfo.numValues += 1;
        streamInfo.lastValueTimestamp = ts;
    }

    private void printBucketState(StreamID streamID) throws RocksDBException {
        StreamInfo streamInfo = streamsInfo.get(streamID);
        System.out.println("Stream " + streamID + " with " + streamInfo.numValues + " elements:");
        for (BucketID bucketID: streamInfo.temporalIndex.values()) {
            System.out.println("\t" + bucketStore.getBucket(streamID, bucketID));
        }
    }

    public void close() throws RocksDBException {
        // FIXME: should wait for any processing appends to terminate first
        persistStreamsInfo();
        bucketStore.close();
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

    @Override
    public long getStreamAge(StreamID streamID) throws StreamException {
        StreamInfo streamInfo;
        synchronized (streamsInfo) {
            streamInfo = streamsInfo.get(streamID);
            if (streamInfo == null) {
                throw new StreamException("attempting to get age of unknown stream " + streamID);
            }
        }
        return streamInfo.lastValueTimestamp.value;
    }

    @Override
    public long getStreamLength(StreamID streamID) throws StreamException {
        StreamInfo streamInfo;
        synchronized (streamsInfo) {
            streamInfo = streamsInfo.get(streamID);
            if (streamInfo == null) {
                throw new StreamException("attempting to get age of unknown stream " + streamID);
            }
        }
        return streamInfo.numValues;
    }

    public static void main(String[] args) {
        SummaryStore store = null;
        try {
            String storeLoc = "/tmp/tdstore";
            // FIXME: add a deleteStream/resetDatabase operation
            Runtime.getRuntime().exec(new String[]{"rm", "-rf", storeLoc}).waitFor();
            //store = new SummaryStore(storeLoc, new ExponentialWBMHWindowingMechanism(3));
            //store = new SummaryStore(storeLoc, new CountBasedWBMH(new ExponentialWindowLengths(2)));
            store = new SummaryStore(new RocksDBBucketStore(storeLoc));
            StreamID streamID = new StreamID(0);
            store.registerStream(streamID, new CountBasedWBMH(new PolynomialWindowLengths(4, 0)));
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
                try {
                    store.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        /*long N = 65536;
        for (int W = 1; W <= 65536; W *= 2) {
            ExponentialWindowLengths.getWindowingOfSize(N, W);
        }*/
    }
}