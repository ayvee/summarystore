package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Time-decayed aggregate storage
 * FIXME:
 * 1. Once a store is closed (via close()), it can only be reopened in read-only
 *    mode (writes will throw a runtime exception). This is because of issues with
 *    serializing the priority queue data structure used in WBMH.
 */
public class SummaryStore implements DataStore {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SummaryStore.class);

    private final String filePrefix;

    private final BucketStore bucketStore;

    /**
     * The buckets proper are stored in bucketStore. We maintain additional in-memory indexes and
     * metadata in StreamInfo to help reads and writes. The append operation keeps StreamInfo
     * consistent with the base data in bucketStore. */
     static class StreamInfo implements Serializable {
        final String filePrefix;
        final long streamID;
        // How many values have we inserted so far?
        long numValues = 0;
        // What was the timestamp of the latest value appended?
        long lastValueTimestamp = -1;

        transient ReadWriteLock lock;

        /* Read index, maps bucket.tStart -> bucketID. Used to answer queries */
        final TreeMap<Long, Long> temporalIndex = new TreeMap<>();
        //transient BTreeMap<Long, Long> temporalIndex;

        /* WindowingMechanism object. Maintains write indexes internally, which will be serialized
         * along with the rest of StreamInfo when persistStreamsInfo() is called */
        transient final WindowingMechanism windowingMechanism;

        void populateTransientFields() {
            lock = new ReentrantReadWriteLock();
            /*DB mapDB = filePrefix != null ?
                    DBMaker.fileDB(filePrefix + ".readIndex").make() :
                    DBMaker.memoryDB().make();
            temporalIndex = mapDB.treeMap("map", Serializer.LONG_DELTA, Serializer.LONG).threadSafeDisable().createOrOpen();*/
        }

        // FIXME: supposed to be called whenever object is deserialized, but FST breaks when we use it
        /*private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
            populateTransientFields();
        }*/

        StreamInfo(String filePrefix, long streamID, WindowingMechanism windowingMechanism) {
            this.filePrefix = filePrefix;
            this.streamID = streamID;
            this.windowingMechanism = windowingMechanism;
            populateTransientFields();
        }
    }

    private final HashMap<Long, StreamInfo> streamsInfo;

    private void persistStreamsInfo() throws RocksDBException {
        bucketStore.putMetadata(streamsInfo);
    }

    /**
     * Create a SummaryStore that stores data and indexes in files/directories that
     * start with filePrefix. To store everything in-memory use a null filePrefix
     */
    public SummaryStore(String filePrefix, long cacheSizePerStream) throws RocksDBException {
        this.filePrefix = filePrefix;
        this.bucketStore = filePrefix != null ?
                new RocksDBBucketStore(filePrefix + ".bucketStore", cacheSizePerStream) :
                new MainMemoryBucketStore();
        Object uncast = bucketStore.getMetadata();
        if (uncast != null) {
            streamsInfo = (HashMap<Long, StreamInfo>) uncast;
            for (StreamInfo si: streamsInfo.values()) {
                si.populateTransientFields();
            }
        } else {
            streamsInfo = new HashMap<>();
        }
    }

    public SummaryStore(String filePrefix) throws RocksDBException {
        this(filePrefix, 0);
    }

    public void registerStream(final long streamID, WindowingMechanism windowingMechanism) throws StreamException, RocksDBException {
        // TODO: also register what data structure we will use for each bucket
        synchronized (streamsInfo) {
            if (streamsInfo.containsKey(streamID)) {
                throw new StreamException("attempting to register streamID " + streamID + " multiple times");
            } else {
                streamsInfo.put(streamID, new StreamInfo(filePrefix, streamID, windowingMechanism));
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
            //BTreeMap<Long, Long> index = streamInfo.temporalIndex;
            TreeMap<Long, Long> index = streamInfo.temporalIndex;
            Long l = index.floorKey(t0); // first bucket with tStart <= t0
            Long r = index.higherKey(t1); // first bucket with tStart > t1
            if (r == null) {
                r = index.lastKey() + 1;
            }
            logger.trace("Overapproximated time range = [{}, {}]", l, r);
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
        if (streamInfo.windowingMechanism == null) {
            throw new UnsupportedOperationException("do not currently support appending to reopened store");
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

    public void printBucketState(long streamID, boolean printPerBucketState) throws RocksDBException {
        StreamInfo streamInfo = streamsInfo.get(streamID);
        System.out.println("Stream " + streamID + " with " + streamInfo.numValues + " elements in " +
                streamInfo.temporalIndex.size() + " windows");
        if (printPerBucketState) {
            for (Object bucketID : streamInfo.temporalIndex.values()) {
                System.out.println("\t" + bucketStore.getBucket(streamID, (long) bucketID));
            }
        }
    }

    public void printBucketState(long streamID) throws RocksDBException {
        printBucketState(streamID, false);
    }

    public void close() throws RocksDBException {
        synchronized (streamsInfo) {
            // wait for all in-process writes and reads to finish, and seal read index
            for (StreamInfo streamInfo: streamsInfo.values()) {
                streamInfo.lock.writeLock().lock();
                //streamInfo.temporalIndex.close();
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
                ret += (long)si.temporalIndex.size() * (long)Bucket.byteCount;
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
    public long getStreamCount(long streamID) throws StreamException {
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
            Runtime.getRuntime().exec(new String[]{"sh", "-c", "rm -rf " + storeLoc + "*"}).waitFor();
            store = new SummaryStore(storeLoc);
            long streamID = 0;
            if (!store.streamsInfo.containsKey(streamID)) {
                //store.registerStream(streamID, new CountBasedWBMH(streamID, new GenericWindowing(new ExponentialWindowLengths(2))));
                Windowing windowing
                        = new GenericWindowing(new ExponentialWindowLengths(2));
                        //= new RationalPowerWindowing(1, 1);
                store.registerStream(streamID, new CountBasedWBMH(streamID, windowing));
                for (long i = 0; i < 1023; ++i) {
                    store.append(streamID, i, i + 1);
                    store.printBucketState(streamID, true);
                }
                //((RationalPowerWindowing) windowing).printDebug();
            } else {
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
