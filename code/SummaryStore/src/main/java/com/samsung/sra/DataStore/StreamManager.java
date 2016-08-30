package com.samsung.sra.DataStore;

import com.google.protobuf.Descriptors;
import com.samsung.sra.DataStore.Aggregates.BloomFilter;
import com.samsung.sra.DataStore.Aggregates.HyperLogLogOperator;
import com.samsung.sra.DataStore.Aggregates.SimpleBloomFilterOperator;
import com.samsung.sra.DataStore.Aggregates.SimpleCountOperator;
import com.samsung.sra.DataStoreExperiments.PairTwo;
import com.samsung.sra.protocol.Summarybucket;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Descriptors.Descriptor;

import com.samsung.sra.protocol.Summarybucket.ProtoBucket;
import com.samsung.sra.protocol.Summarybucket.ProtoSimpleCount;
import com.samsung.sra.protocol.Summarybucket.ProtoSimpleBloomFilter;


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

    String a = java.lang.Long.class.toString();
    String b = BloomFilter.class.toString();
    //String x = java.lang.Long.class.toString();
    //String y = BloomFilter.class.toString();

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

    // for testing only
    Random rand = new Random();

    transient BucketStore bucketStore;

    transient HashMap<String, PairTwo<WindowOperator, Integer>> objectStringMap = new HashMap<>();

    //FIXME: NA String map; needs to contain string names for operator classes
    void constructOperatorNameMap() {
        int op = 0;
        String[] opNames = new String[operators.length];
        for (int i = 0; i < operators.length; ++i) {
            objectStringMap.put(operators[i].getClass().toString(), new PairTwo<>(operators[i], i));
        }

        opNames[++op] = SimpleCountOperator.class.toString();
        opNames[++op] = HyperLogLogOperator.class.toString();
        opNames[op++] = SimpleBloomFilterOperator.class.toString();

        // ensure that all operators have been added to this array
        assert (op == operators.length);

    }

    //FIXME: NA; part of String map code
    int lookupOperatorTypeByClass(WindowOperator wo) {
        return 0;
    }

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

        /** no longer needed
         * bytesPerBucket = Bucket.METADATA_BYTECOUNT +
         * Stream.of(operators).mapToInt(WindowOperator::getBytecount).sum();
         */

        //FIXME: NA; part of String map code
        int i = 0;
        for (WindowOperator operator : operators) {
            objectStringMap.put(operator.getClass().toString(), new PairTwo<>(operator, ++i));
        }
        // display map
        //System.out.println("Window Operator Map: " + objectStringMap.toString());
    }

    // TODO: add assertions

    void append(long ts, Object value) throws RocksDBException, StreamException {
        if (ts <= stats.lastArrivalTimestamp) throw new StreamException("out-of-order insert in stream " + streamID);
        stats.append(ts, value);
        windowingMechanism.append(this, ts, value);
    }

    Object query(int operatorNum, long t0, long t1, Object[] queryParams) throws RocksDBException {
        if (t0 > stats.lastArrivalTimestamp) {
            logger.warn("Returning empty query result due to t0: " + t0);
            return operators[operatorNum].getEmptyQueryResult();
        } else if (t1 > stats.lastArrivalTimestamp) {
            logger.warn("Resetting t1: " + t1 + " to " + stats.lastArrivalTimestamp);
            t1 = stats.lastArrivalTimestamp;
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

    void insertValueIntoBucket(Bucket bucket, long ts, Object value) {
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

        ProtoBucket.Builder protoBucket = null;

        try {
            protoBucket = ProtoBucket.newBuilder().
                    setThisBucketid(bucket.thisBucketID).
                    setNextBucketID(bucket.nextBucketID).
                    setPrevBucketID(bucket.prevBucketID).
                    setTStart(bucket.tStart).setTEnd(bucket.tEnd).
                    setCStart(bucket.cStart).setCEnd(bucket.cEnd);
        } catch (Exception e) {
            logger.error("Exception in serializing bucket ");
            e.printStackTrace();
        }

        for (int op = 0; op < operators.length; ++op) {

            //objectStringMap.get(bucket.aggregates[op].getClass().toString());

            try {
                if (bucket.aggregates[op] != null) {
                    if (a.equalsIgnoreCase(bucket.aggregates[op].getClass().toString())) {
                        //logger.debug("Simple Count for aggr[" + op + "] in Bucket " + bucket.thisBucketID);

                        //NA: solely for testing
                        //bucket.aggregates[op] = rand.nextLong();

                        protoBucket.setPcount(((ProtoSimpleCount.Builder)
                                operators[op].protofy(bucket.aggregates[op])).build());

                    } else if (b.equalsIgnoreCase(bucket.aggregates[op].getClass().toString())) {
                        //logger.debug("Simple Bloom for aggr[" + op + "] in Bucket " + bucket.thisBucketID);

                        protoBucket.setPbloom(((ProtoSimpleBloomFilter.Builder)
                                operators[op].protofy(bucket.aggregates[op])).build());
                    }
                } else {
                    logger.error("aggr[" + op + "] in Bucket " + bucket.thisBucketID + " before serialize is NULL");
                }
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
        int op = 0;

        /** find number of aggregates in protobucket or use operators.length
         * eventually this assign and check should be dropped since the serialized object
         * is self describing; just check until iterate over all "hasProto*"
         */
        bucket.aggregates = new Object[operators.length];

        if (protoBucket.hasPcount() && op < operators.length) {
            bucket.aggregates[op] = operators[op].deprotofy(protoBucket.getPcount().toBuilder());
            op++;
        }

        if (protoBucket.hasPbloom() && op < operators.length) {
            bucket.aggregates[op] = operators[op].deprotofy(protoBucket.getPbloom().toBuilder());
            op++;
        }

        // must deserialize all valid operators that were serialized
        if(op!=operators.length)
            logger.error("Error: deserializing less operators than needed");
        assert(op == operators.length);

        /** THIS IS TO BE DELETED
         for (int op = 0; op < operators.length; ++op) {
             PairTwo<WindowOperator, Integer> pairTwo1 = objectStringMap.get(operators[op].getClass().toString());
             switch(pairTwo1.second()) {}
             PairTwo<WindowOperator, Integer> pairTwo2 = objectStringMap.get(protoBucket.getPcount());
             logger.debug("DESER == x: " + x + " y: " + y + " class: " + operators[op].getClass().toString());

             if(x.equalsIgnoreCase(operators[op].getClass().toString())) {
                 logger.debug("Attempting Deser in " + operators[op].getClass().getName());
                 bucket.aggregates[op] = operators[op].deprotofy(protoBucket.getPcount().toBuilder());
             }
             else if(y.equalsIgnoreCase(operators[op].getClass().toString())) {
                logger.debug("Attempting Deser in " + operators[op].getClass().getName());
                bucket.aggregates[op] = operators[op].deprotofy(protoBucket.getPbloom().toBuilder());
             }
             //else if(c.equalsIgnoreCase(operators[op].getClass().toString()))
             //  bucket.aggregates[op] = operators[op].deprotofy(protoBucket.getPsum().toBuilder());

             if (bucket.aggregates[op] != null)
                logger.debug("Op " + operators[op].getClass().getName() + " in bucket "
                    + bucket.thisBucketID + " after deser: " + bucket.aggregates[op].toString());
             else
                logger.debug("Op " + operators[op].getClass().getName() + " in bucket "
                    + bucket.thisBucketID + " after deser is null ");
             }
         */

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
