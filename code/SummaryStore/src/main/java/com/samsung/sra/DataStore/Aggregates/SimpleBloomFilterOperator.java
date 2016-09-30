package com.samsung.sra.DataStore.Aggregates;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.samsung.sra.DataStore.*;
import com.samsung.sra.protocol.Summarybucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Created by n.agrawal1 on 7/18/16.
 * This operator uses the BloomFilter implementation from the stream-lib library
 * https://github.com/addthis/stream-lib
 *
 * Bloom Filter:
 * https://github.com/addthis/stream-lib/tree/master/src/main/java/com/clearspring/analytics/stream/membership
 */

//AVRE: Bloomfilter, Long, Boolean, Double
public class SimpleBloomFilterOperator implements WindowOperator<BloomFilter, Boolean, Double>{


    private int filterSize = 128; //2^15;
    private int nrHashes = 7;
    private static Logger logger = LoggerFactory.getLogger(SimpleBloomFilterOperator.class);

    public SimpleBloomFilterOperator() {
       //use default values; currently not used
    }


    public SimpleBloomFilterOperator(int filterSize, int nrHashes) {
        this.filterSize = filterSize;
        this.nrHashes = nrHashes;
    }

    private static final List<String> supportedQueries = Collections.singletonList("simplebloom");

    @Override
    public List<String> getSupportedQueryTypes() {
        return supportedQueries;
    }

    /**
     * Create an empty aggregate (containing zero elements)
     *
     */
    @Override
    public BloomFilter createEmpty() {
        //logger.debug("Creating new Empty BF: "+filterSize +":" + nrHashes);
        return new BloomFilter(filterSize, nrHashes);
    }

    /**
     * Union a sequence of aggregates into one
     *
     * @param aggrs stream of aggregate operators
     */
    @Override
    public BloomFilter merge(Stream<BloomFilter> aggrs) {

        BloomFilter newBloomFilter = createEmpty();
        //logger.debug(newBloomFilter.toString());

        for (BloomFilter bfItem : (Iterable<BloomFilter>) aggrs::iterator) {
            //logger.debug("Item: " + bfItem.toString());

            newBloomFilter = BloomFilter.unionOf(newBloomFilter, bfItem);
        }

        //logger.debug(newBloomFilter.toString());

        return newBloomFilter;
    }

    /**
     * Insert val into aggr and return the updated aggregate
     *
     * @param aggr
     * @param timestamp
     * @param val
     */
    @Override
    public BloomFilter insert(BloomFilter aggr, long timestamp, Object[] val) {
        byte[] bytes = new byte[Long.SIZE];
        Utilities.longToByteArray((long)val[0],bytes,0);
        aggr.add(bytes);
        return aggr;
    }

    @Override
    public Estimator<Boolean, Double> buildEstimator(StreamStatistics streamStatistics,
                    long T0, long T1, Stream<Bucket> buckets, Function<Bucket, BloomFilter> aggregateRetriever) {
        return null;
    }

    /**
     *     default Estimator<R, E> buildEstimator(StreamStatistics streamStats,
     long T0, long T1, Stream<Bucket> buckets, Function<Bucket, A> aggregateRetriever        s) {
     return null;
     }
     */


    /**
     * Retrieve aggregates from a set of buckets and do a combined query over them. We pass full
     * Bucket objects instead of specific Aggregate objects of type A to allow query() to access
     * Bucket metadata.
     * TODO: pass an additional Function<Bucket, BucketMetadata> metadataRetriever as argument,
     * instead of letting query() manhandle Bucket objects
     *
     * @param buckets
     * @param bloomRetriever
     * @param t0
     * @param t1
     * @param params
     */
    @Override
    public ResultError<Boolean, Double> query(StreamStatistics streamStatistics, long T0, long T1,
         Stream<Bucket> buckets, Function<Bucket, BloomFilter> bloomRetriever, long t0, long t1, Object... params) {

        BloomFilter newBloom = createEmpty();
        for(Bucket bucketItem : (Iterable<Bucket>) buckets::iterator) {
            newBloom = BloomFilter.unionOf(newBloom, bloomRetriever.apply(bucketItem));
        }

        byte[] bytes = new byte[Long.SIZE];
        Utilities.longToByteArray((long) params[0],bytes,0);
        logger.debug("Bloom Query for value: " + params[0]);
        return new ResultError<>(newBloom.isPresent(bytes), 0.0);
    }


    /**
     * Return the default answer to a query on an empty aggregate (containing zero elements)
     */
    @Override
    public ResultError<Boolean, Double> getEmptyQueryResult() {
        return new ResultError<>(false, 0.0);
    }


    public static class SimpleBloomEstimator implements Estimator<Boolean, Double> {

        @Override
        public ResultError<Boolean, Double> estimate(long t0, long t1, Object... params) {
            return null;
        }

    }

    @Override
    public Message.Builder protofy(BloomFilter aggr) {

        // First construct builder for BitSet
        Summarybucket.ProtoBitset.Builder pbBuilder = Summarybucket.ProtoBitset.
                newBuilder().
                addBitsetbytes(ByteString.copyFrom(aggr.filter().toByteArray()));

        // return builder for all three: bitset, filtersize, num hashes
        return  Summarybucket.
                ProtoSimpleBloomFilter.
                newBuilder().
                setBitset(pbBuilder).
                setFiltersize(aggr.getFilterSize()).
                setNumhashes(aggr.getHashCount());
    }

    @Override
    public BloomFilter deprotofy(Message.Builder builder) {

        // BitSet is a nested proto message
        BloomFilter bf = new BloomFilter (
                ((Summarybucket.ProtoSimpleBloomFilter.Builder) builder).getNumhashes(),
                BitSet.valueOf(((Summarybucket.ProtoSimpleBloomFilter.Builder) builder).getBitset().toByteArray()),
                ((Summarybucket.ProtoSimpleBloomFilter.Builder) builder).getFiltersize());

        return bf;
    }
}
