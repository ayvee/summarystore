package com.samsung.sra.DataStore;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.rocksdb.RocksDBException;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Time-decayed aggregate storage
 */
public class SummaryStore implements DataStore {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SummaryStore.class);

    private final BucketStore bucketStore;

    /**
     * The buckets proper are stored in bucketStore. We maintain additional in-memory indexes and
     * metadata in StreamInfo to help reads and writes. The append operation keeps StreamInfo
     * consistent with the base data in bucketStore. */
     static class StreamInfo implements Serializable {
        final long streamID;
        // How many values have we inserted so far?
        long numValues = 0;
        // What was the timestamp of the latest value appended?
        long lastValueTimestamp = -1;

        final ReadWriteLock lock = new ReentrantReadWriteLock();

        /* Read index, maps bucket.tStart -> bucketID. Used to answer queries */
        //final TreeMap<Timestamp, BucketID> temporalIndex = new TreeMap<>();
        final BTreeMap<Long, Long> temporalIndex;

        /* WindowingMechanism object. Maintains write indexes internally, which will be serialized
         * along with the rest of StreamInfo when persistStreamsInfo() is called */
        final WindowingMechanism windowingMechanism;

        StreamInfo(long streamID, WindowingMechanism windowingMechanism) {
            this.streamID = streamID;
            this.windowingMechanism = windowingMechanism;
            DB mapDB = DBMaker.memoryDB().make();
            temporalIndex = mapDB.treeMap("map", Serializer.LONG, Serializer.LONG).createOrOpen();
        }
    }

    private final HashMap<Long, StreamInfo> streamsInfo;

    private void persistStreamsInfo() throws RocksDBException {
        bucketStore.putIndexes(streamsInfo);
    }

    public SummaryStore(BucketStore bucketStore) throws RocksDBException {
        this.bucketStore = bucketStore;
        Object uncast = bucketStore.getIndexes();
        streamsInfo = uncast != null ?
                (HashMap<Long, StreamInfo>)uncast :
                new HashMap<>();
    }

    public void registerStream(final long streamID, WindowingMechanism windowingMechanism) throws StreamException, RocksDBException {
        // TODO: also register what data structure we will use for each bucket
        synchronized (streamsInfo) {
            if (streamsInfo.containsKey(streamID)) {
                throw new StreamException("attempting to register streamID " + streamID + " multiple times");
            } else {
                streamsInfo.put(streamID, new StreamInfo(streamID, windowingMechanism));
            }
        }
    }

    @Override
    public Object query(long streamID, long t0, long t1, QueryType queryType, Object[] queryParams) throws StreamException, QueryException, RocksDBException {
        if (t0 < 0 || t0 > t1) {
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
            if (t1 > streamInfo.lastValueTimestamp) {
                throw new QueryException("[" + t0 + ", " + t1 + "] is not a valid time interval");
            }
            BTreeMap<Long, Long> index = streamInfo.temporalIndex;
            Long l = index.floorKey(t0); // first bucket with tStart <= t0
            Long r = index.higherKey(t1); // first bucket with tStart > t1
            logger.trace("Overapproximated time range = [{}, {}]", l, r);
            if (r == null) {
                r = index.lastKey();
            }
            // Query on all buckets with l <= tStart < r
            SortedMap<Long, Long> spanningBucketsIDs = index.subMap(l, true, r, false);
            Bucket first = null;
            List<Bucket> rest = new ArrayList<>();
            // TODO: RocksDB multiget
            for (Long bucketID: spanningBucketsIDs.values()) {
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

    public void append(long streamID, long ts, Object value) throws StreamException, RocksDBException {
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
            streamInfo.windowingMechanism.append(this, ts, value);
            ++streamInfo.numValues;
            streamInfo.lastValueTimestamp = ts;
        } finally {
            streamInfo.lock.writeLock().unlock();
        }
    }

    /* getBucket and putBucket are for use by WindowingMechanism objects. They update both the underlying
       bucketStore and the temporal index we maintain. The stream-write lock should be acquired before
       calling these functions */

    Bucket getBucket(long streamID, long bucketID, boolean delete) throws RocksDBException {
        Bucket bucket = bucketStore.getBucket(streamID, bucketID, delete);
        if (delete) {
            streamsInfo.get(streamID).temporalIndex.remove(bucket.tStart);
        }
        return bucket;
    }

    Bucket getBucket(long streamID, long bucketID) throws RocksDBException {
        return getBucket(streamID, bucketID, false);
    }

    void putBucket(long streamID, long bucketID, Bucket bucket) throws RocksDBException {
        streamsInfo.get(streamID).temporalIndex.put(bucket.tStart, bucketID);
        bucketStore.putBucket(streamID, bucketID, bucket);
    }

    public void printBucketState(long streamID) throws RocksDBException {
        StreamInfo streamInfo = streamsInfo.get(streamID);
        System.out.println("Stream " + streamID + " with " + streamInfo.numValues + " elements:");
        for (Object bucketID: streamInfo.temporalIndex.values()) {
            System.out.println("\t" + bucketStore.getBucket(streamID, (long)bucketID));
        }
    }

    public void close() throws RocksDBException {
        synchronized (streamsInfo) {
            // wait for all in-process writes and reads to finish
            for (StreamInfo streamInfo: streamsInfo.values()) {
                streamInfo.lock.writeLock().lock();
            }
            // at this point all operations on existing streams will be blocked
            // TODO: lock out creating new streams
            persistStreamsInfo();
            bucketStore.close();
        }
    }

    @Override
    public long getStoreSizeInBytes() {
        long ret = 0;
        for (StreamInfo si: streamsInfo.values()) {
            si.lock.readLock().lock();
            try {
                // FIXME: this does not account for the size of the time/count markers tracked by the index
                ret += si.temporalIndex.size() * (8 + 8 + Bucket.byteCount);
            } finally {
                si.lock.readLock().unlock();
            }
        }
        return ret;
    }

    @Override
    public long getStreamAge(long streamID) throws StreamException {
        StreamInfo streamInfo;
        synchronized (streamsInfo) {
            streamInfo = streamsInfo.get(streamID);
            if (streamInfo == null) {
                throw new StreamException("attempting to get age of unknown stream " + streamID);
            }
        }
        streamInfo.lock.readLock().lock();
        try {
            return streamInfo.lastValueTimestamp;
        } finally {
            streamInfo.lock.readLock().unlock();
        }
    }

    @Override
    public long getStreamLength(long streamID) throws StreamException {
        StreamInfo streamInfo;
        synchronized (streamsInfo) {
            streamInfo = streamsInfo.get(streamID);
            if (streamInfo == null) {
                throw new StreamException("attempting to get age of unknown stream " + streamID);
            }
        }
        streamInfo.lock.readLock().lock();
        try {
            return streamInfo.numValues;
        } finally {
            streamInfo.lock.readLock().unlock();
        }
    }

    public static void main(String[] args) {
        SummaryStore store = null;
        try {
            String storeLoc = "/tmp/tdstore";
            // FIXME: add a deleteStream/resetDatabase operation
            Runtime.getRuntime().exec(new String[]{"rm", "-rf", storeLoc}).waitFor();
            store = new SummaryStore(new RocksDBBucketStore(storeLoc));
            long streamID = 0;
            store.registerStream(streamID, new CountBasedWBMH(streamID, new PolynomialWindowLengths(4, 0)));
            //store.registerStream(streamID, new CountBasedWBMH(streamID, new ExponentialWindowLengths(2)));
            for (long i = 0; i < 20; ++i) {
                store.append(streamID, i, i + 1);
                store.printBucketState(streamID);
            }
            long t0 = 0, t1 = 4;
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
    }
}