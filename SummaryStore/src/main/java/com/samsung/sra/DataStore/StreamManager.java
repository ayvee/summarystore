package com.samsung.sra.DataStore;

import com.google.protobuf.InvalidProtocolBufferException;
import com.samsung.sra.protocol.Summarybucket.ProtoBucket;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
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
 * <p>
 * StreamManagers do not handle synchronization, callers are expected to manage the lock
 * object here.
 */
class StreamManager implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(StreamManager.class);
    final long streamID;
    final WindowOperator[] operators;
    final StreamStatistics stats;

    final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Read index, maps bucket.tStart -> bucketID. Used to answer queries
     */
    final TreeMap<Long, Long> temporalIndex = new TreeMap<>();
    //transient BTreeMap<Long, Long> temporalIndex;

    /**
     * WindowingMechanism object. Maintains write indexes internally, which SummaryStore
     * will persist to disk along with the rest of StreamManager
     */
    final WindowingMechanism windowingMechanism;

    transient BucketStore bucketStore;
    transient ExecutorService executorService;

    void populateTransientFields(BucketStore bucketStore, ExecutorService executorService) {
        this.bucketStore = bucketStore;
        this.executorService = executorService;
        windowingMechanism.populateTransientFields();
    }

    StreamManager(BucketStore bucketStore, ExecutorService executorService,
                  long streamID,
                  WindowingMechanism windowingMechanism, WindowOperator... operators) {
        this.streamID = streamID;
        this.bucketStore = bucketStore;
        this.executorService = executorService;
        this.windowingMechanism = windowingMechanism;
        this.operators = operators;
        this.stats = new StreamStatistics();
    }

    // TODO: add assertions

    void append(long ts, Object[] value) throws RocksDBException, StreamException {
        if (ts <= stats.getTimeRangeEnd()) throw new StreamException("out-of-order insert in stream " + streamID +
                ": <ts, val> = <" + ts + ", " + value + ">, last arrival = " + stats.getTimeRangeEnd());
        stats.append(ts, value);
        windowingMechanism.append(this, ts, value);
    }

    Object query(int operatorNum, long t0, long t1, Object[] queryParams) throws RocksDBException {
        long T0 = stats.getTimeRangeStart(), T1 = stats.getTimeRangeEnd();
        if (t0 > T1) {
            return operators[operatorNum].getEmptyQueryResult();
        } else if (t0 < T0) {
            t0 = T0;
        }
        if (t1 > T1) {
            t1 = T1;
        }
        //BTreeMap<Long, Long> index = streamManager.temporalIndex;
        Long l = temporalIndex.floorKey(t0); // first bucket with tStart <= t0
        Long r = temporalIndex.higherKey(t1); // first bucket with tStart > t1
        if (r == null) {
            r = temporalIndex.lastKey() + 1;
        }
        //logger.debug("Overapproximated time range = [{}, {})", l, r);

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
            return operators[operatorNum].query(stats, l, r - 1, buckets, retriever, t0, t1, queryParams);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof RocksDBException) {
                throw (RocksDBException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    void insertValueIntoBucket(Bucket bucket, long ts, Object[] value) {
        assert bucket.tStart <= ts && (bucket.tEnd == -1 || ts <= bucket.tEnd)
                && operators.length == bucket.aggregates.length;
        for (int i = 0; i < operators.length; ++i) {
            bucket.aggregates[i] = operators[i].insert(bucket.aggregates[i], ts, value);
        }
    }

    /**
     * Replace buckets[0] with union(buckets)
     */
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


    byte[] serializeBucketProto(Bucket bucket) {

        if (bucket == null) {
            logger.error("NULL Bucket about to be serialized");
        }

        ProtoBucket.Builder protoBucket;

        try {
            protoBucket = ProtoBucket.newBuilder().
                    setThisBucketid(bucket.thisBucketID).
                    setNextBucketID(bucket.nextBucketID).
                    setPrevBucketID(bucket.prevBucketID).
                    setTStart(bucket.tStart).setTEnd(bucket.tEnd).
                    setCStart(bucket.cStart).setCEnd(bucket.cEnd);
        } catch (Exception e) {
            logger.error("Exception in serializing bucket", e);
            throw new RuntimeException(e);
        }

        for (int op = 0; op < operators.length; ++op) {

            //objectStringMap.get(bucket.aggregates[op].getClass().toString());

            try {
                assert bucket.aggregates[op] != null;
                protoBucket.addOperator(operators[op].protofy(bucket.aggregates[op]));
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        return protoBucket.build().toByteArray();
    }

    Bucket deserializeBucketProto(ProtoBucket protoBucket) {

        Bucket bucket = new Bucket();
        bucket.thisBucketID = protoBucket.getThisBucketid();
        bucket.prevBucketID = protoBucket.getPrevBucketID();
        bucket.nextBucketID = protoBucket.getNextBucketID();
        bucket.tStart = protoBucket.getTStart();
        bucket.tEnd = protoBucket.getTEnd();
        bucket.cStart = protoBucket.getCStart();
        bucket.cEnd = protoBucket.getCEnd();

        /** find number of aggregates in protobucket or use operators.length
         * eventually this assign and check should be dropped since the serialized object
         * is self describing; just check until iterate over all "hasProto*"
         */
        bucket.aggregates = new Object[operators.length];

        assert protoBucket.getOperatorCount() == operators.length;
        for (int op = 0; op < operators.length; ++op) {
            bucket.aggregates[op] = operators[op].deprotofy(protoBucket.getOperator(op));
        }

        return bucket;
    }


    // just wrappers for backward compatibility
    byte[] serializeBucket(Bucket bucket) {
        return serializeBucketProto(bucket);
    }

    Bucket deserializeBucket(byte[] bytes) {
        Bucket bucket=null;
        try {
            bucket = deserializeBucketProto(ProtoBucket.parseFrom(bytes));
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return bucket;
    }
}
