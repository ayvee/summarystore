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
        final StreamID streamID;
        // How many values have we inserted so far?
        long numValues = 0;
        // What was the timestamp of the latest value appended?
        Timestamp lastValueTimestamp = null;

        final ReadWriteLock lock = new ReentrantReadWriteLock();

        /* Read index, maps bucket.tStart -> bucketID. Used to answer queries */
        //final TreeMap<Timestamp, BucketID> temporalIndex = new TreeMap<>();
        final BTreeMap<Long, Long> temporalIndex;

        /* WindowingMechanism object. Maintains write indexes internally, which will be serialized
         * along with the rest of StreamInfo when persistStreamsInfo() is called */
        final WindowingMechanism windowingMechanism;

        StreamInfo(StreamID streamID, WindowingMechanism windowingMechanism) {
            this.streamID = streamID;
            this.windowingMechanism = windowingMechanism;
            DB mapDB = DBMaker.memoryDB().make();
            temporalIndex = mapDB.treeMap("map", Serializer.LONG, Serializer.LONG).createOrOpen();
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
            BTreeMap<Long, Long> index = streamInfo.temporalIndex;
            Long l = index.floorKey(t0.value); // first bucket with tStart <= t0
            Long r = index.higherKey(t1.value); // first bucket with tStart > t1
            logger.trace("Overapproximated time range = [{}, {}]", l, r);
            if (r == null) {
                r = index.lastKey();
            }
            // Query on all buckets with l <= tStart < r
            SortedMap<Long, Long> spanningBucketsIDs = index.subMap(l, true, r, false);
            Bucket first = null;
            List<Bucket> rest = new ArrayList<>();
            // TODO: RocksDB multiget
            for (Long bucketIDlong: spanningBucketsIDs.values()) {
                Bucket bucket = bucketStore.getBucket(streamID, new BucketID(bucketIDlong));
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
            if (logger.isDebugEnabled() && streamInfo.numValues % 1000000 == 0) {
                logger.debug("size of temporal index = {}", streamInfo.temporalIndex.size());
            }
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

    Bucket getBucket(StreamID streamID, BucketID bucketID, boolean delete) throws RocksDBException {
        Bucket bucket = bucketStore.getBucket(streamID, bucketID, delete);
        if (delete) {
            streamsInfo.get(streamID).temporalIndex.remove(bucket.tStart.value);
        }
        return bucket;
    }

    Bucket getBucket(StreamID streamID, BucketID bucketID) throws RocksDBException {
        return getBucket(streamID, bucketID, false);
    }

    void putBucket(StreamID streamID, BucketID bucketID, Bucket bucket) throws RocksDBException {
        streamsInfo.get(streamID).temporalIndex.put(bucket.tStart.value, bucketID.id);
        bucketStore.putBucket(streamID, bucketID, bucket);
    }

    public void printBucketState(StreamID streamID) throws RocksDBException {
        StreamInfo streamInfo = streamsInfo.get(streamID);
        System.out.println("Stream " + streamID + " with " + streamInfo.numValues + " elements:");
        for (Object bucketIDlong: streamInfo.temporalIndex.values()) {
            System.out.println("\t" + bucketStore.getBucket(streamID, new BucketID((Long)bucketIDlong)));
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

    public long getStoreSizeInBytes() {
        long ret = 0;
        for (StreamInfo si: streamsInfo.values()) {
            si.lock.readLock().lock();
            try {
                // FIXME: this does not account for the size of the time/count markers tracked by the index
                ret += si.temporalIndex.size() * (StreamID.byteCount + BucketID.byteCount + Bucket.byteCount);
            } finally {
                si.lock.readLock().unlock();
            }
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
        streamInfo.lock.readLock().lock();
        try {
            return streamInfo.lastValueTimestamp.value;
        } finally {
            streamInfo.lock.readLock().unlock();
        }
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
            StreamID streamID = new StreamID(0);
            store.registerStream(streamID, new CountBasedWBMH(streamID, new PolynomialWindowLengths(4, 0)));
            //store.registerStream(streamID, new CountBasedWBMH(streamID, new ExponentialWindowLengths(2)));
            for (long i = 0; i < 20; ++i) {
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
    }
}