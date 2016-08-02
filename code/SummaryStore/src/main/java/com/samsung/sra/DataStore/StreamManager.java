package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Handles all operations on a particular stream, both the southbound Bucket get, create
 * and merge operations initiated by WBMH as well as the northbound append() and query()
 * operations initiated by clients via SummaryStore. Stores aggregate per-stream information
 * as well as in-memory indexes. The buckets themselves are not stored directly here, they
 * go into the underlying bucketStore.
 *
 * StreamManagers do not handle synchronization, callers are expected to manage the lock
 * object here.
 */
class StreamManager implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(StreamManager.class);
    final long streamID;
    final WindowOperator[] operators;
    final StreamStatistics stats;

    final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** Read index, maps bucket.tStart -> bucketID. Used to answer queries */
    final TreeMap<Long, Long> temporalIndex = new TreeMap<>();
    //transient BTreeMap<Long, Long> temporalIndex;

    /** WindowingMechanism object. Maintains write indexes internally, which SummaryStore
     * will persist to disk along with the rest of StreamManager */
    final WindowingMechanism windowingMechanism;

    final int bytesPerBucket;

    transient BucketStore bucketStore;

    void populateTransientFields(BucketStore bucketStore) {
        this.bucketStore = bucketStore;
        windowingMechanism.populateTransientFields();
    }

    StreamManager(BucketStore bucketStore, long streamID,
                  WindowingMechanism windowingMechanism, WindowOperator... operators) {
        this.streamID = streamID;
        this.bucketStore = bucketStore;
        this.windowingMechanism = windowingMechanism;
        this.operators = operators;
        this.stats = new StreamStatistics();
        bytesPerBucket = Bucket.METADATA_BYTECOUNT +
                Stream.of(operators).mapToInt(WindowOperator::getBytecount).sum();
    }

    // TODO: add assertions

    void append(long ts, Object value) throws RocksDBException, StreamException {
        if (ts <= stats.lastArrivalTimestamp) throw new StreamException("out-of-order insert in stream " + streamID);
        stats.append(ts, value);
        windowingMechanism.append(this, ts, value);
    }

    Object query(int operatorNum, long t0, long t1, Object[] queryParams) throws RocksDBException {
        if (t0 > stats.lastArrivalTimestamp) {
            return operators[operatorNum].getEmptyQueryResult();
        } else if (t1 > stats.lastArrivalTimestamp) {
            t1 = stats.lastArrivalTimestamp;
        }
        //BTreeMap<Long, Long> index = streamManager.temporalIndex;
        Long l = temporalIndex.floorKey(t0); // first bucket with tStart <= t0
        Long r = temporalIndex.higherKey(t1); // first bucket with tStart > t1
        if (r == null) {
            r = temporalIndex.lastKey() + 1;
        }
        logger.trace("Overapproximated time range = [{}, {}]", l, r);
        // Query on all buckets with l <= tStart < r
        SortedMap<Long, Long> spanningBucketsIDs = temporalIndex.subMap(l, true, r, false);
        Stream<Bucket> buckets = spanningBucketsIDs.values().stream().map(bucketID -> {
            try {
                return bucketStore.getBucket(this, bucketID, false);
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        });
        try {
            Function<Bucket, Object> retriever = b -> b.aggregates[operatorNum];
            return operators[operatorNum].query(stats, l, r, buckets, retriever, t0, t1, queryParams);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof RocksDBException) {
                throw (RocksDBException)e.getCause();
            } else {
                throw e;
            }
        }
    }

    void insertValueIntoBucket(Bucket bucket, long ts, Object value) {
        assert bucket.tStart <= ts && (bucket.tEnd == -1 || ts <= bucket.tEnd) && operators.length == bucket.aggregates.length;
        for (int i = 0; i < operators.length; ++i) {
            bucket.aggregates[i] = operators[i].insert(bucket.aggregates[i], ts, value);
        }
    }

    /** Replace buckets[0] with union(buckets) */
    void mergeBuckets(Bucket... buckets) {
        if (buckets.length == 0) return;
        buckets[0].cEnd = buckets[buckets.length - 1].cEnd;
        buckets[0].tEnd = buckets[buckets.length - 1].tEnd;
        for (int opNum = 0; opNum < operators.length; ++opNum) {
            final int i = opNum; // work around Java dumbness re stream.map arguments
            buckets[0].aggregates[i] = operators[i].merge(Stream.of(buckets).map(b -> b.aggregates[i]));
        }
    }

    Bucket createEmptyBucket(long prevBucketID, long thisBucketID, long nextBucketID,
                             long tStart, long tEnd, long cStart, long cEnd) {
        return new Bucket(operators, prevBucketID, thisBucketID, nextBucketID, tStart, tEnd, cStart, cEnd);
    }

    Bucket getBucket(long bucketID, boolean delete) throws RocksDBException {
        Bucket bucket = bucketStore.getBucket(this, bucketID, delete);
        if (delete) {
            temporalIndex.remove(bucket.tStart);
        }
        return bucket;
    }

    Bucket getBucket(long bucketID) throws RocksDBException {
        return getBucket(bucketID, false);
    }

    void putBucket(long bucketID, Bucket bucket) throws RocksDBException {
        temporalIndex.put(bucket.tStart, bucketID);
        bucketStore.putBucket(this, bucketID, bucket);
    }

    byte[] serializeBucket(Bucket bucket) {
        byte[] bytes = new byte[bytesPerBucket];
        // metadata
        Utilities.longToByteArray(bucket.prevBucketID, bytes, 0);
        Utilities.longToByteArray(bucket.thisBucketID, bytes, 8);
        Utilities.longToByteArray(bucket.nextBucketID, bytes, 16);
        Utilities.longToByteArray(bucket.tStart, bytes, 24);
        Utilities.longToByteArray(bucket.tEnd, bytes, 32);
        Utilities.longToByteArray(bucket.cStart, bytes, 40);
        Utilities.longToByteArray(bucket.cEnd, bytes, 48);
        int pos = 56;
        for (int op = 0; op < operators.length; ++op) {
            operators[op].serialize(bucket.aggregates[op], bytes, pos);
            pos += operators[op].getBytecount();
        }
        assert pos == bytesPerBucket;
        return bytes;
    }

    Bucket deserializeBucket(byte[] bytes) {
        assert bytes.length == bytesPerBucket;
        Bucket bucket = new Bucket();
        bucket.prevBucketID = Utilities.byteArrayToLong(bytes, 0);
        bucket.thisBucketID = Utilities.byteArrayToLong(bytes, 8);
        bucket.nextBucketID = Utilities.byteArrayToLong(bytes, 16);
        bucket.tStart = Utilities.byteArrayToLong(bytes, 24);
        bucket.tEnd = Utilities.byteArrayToLong(bytes, 32);
        bucket.cStart = Utilities.byteArrayToLong(bytes, 40);
        bucket.cEnd = Utilities.byteArrayToLong(bytes, 48);
        bucket.aggregates = new Object[operators.length];
        int pos = 56;
        for (int op = 0; op < operators.length; ++op) {
            bucket.aggregates[op] = operators[op].deserialize(bytes, pos);
            pos += operators[op].getBytecount();
        }
        assert pos == bytesPerBucket;
        return bucket;
    }
}
