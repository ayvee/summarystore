package com.samsung.sra.DataStore.Aggregates;

import com.google.protobuf.ByteString;
import com.samsung.sra.DataStore.*;
import com.samsung.sra.protocol.Summarybucket;
import com.samsung.sra.protocol.Summarybucket.ProtoOperator;
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
public class BloomFilterOperator implements WindowOperator<BloomFilter, Boolean, Double>{


    private int filterSize = 128; //2^15;
    private int nrHashes = 7;
    private static Logger logger = LoggerFactory.getLogger(BloomFilterOperator.class);

    public BloomFilterOperator() {
       //use default values; currently not used
    }


    public BloomFilterOperator(int filterSize, int nrHashes) {
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
        return new ResultError<>(newBloom.isPresent(bytes), null);
    }


    /**
     * Return the default answer to a query on an empty aggregate (containing zero elements)
     */
    @Override
    public ResultError<Boolean, Double> getEmptyQueryResult() {
        return new ResultError<>(false, 0.0);
    }


    @Override
    public ProtoOperator.Builder protofy(BloomFilter aggr) {

        // First construct builder for BitSet
        Summarybucket.ProtoBitset.Builder pbBuilder = Summarybucket.ProtoBitset.
                newBuilder().
                addBitsetbytes(ByteString.copyFrom(aggr.filter().toByteArray()));

        // Base Bloom filter builder
        Summarybucket.ProtoSimpleBloomFilter.Builder bfBuilder = Summarybucket
                .ProtoSimpleBloomFilter
                .newBuilder()
                .setBitset(pbBuilder)
                .setFiltersize(aggr.getFilterSize())
                .setNumhashes(aggr.getHashCount());

        // Wrap inside operator
        return Summarybucket
                .ProtoOperator
                .newBuilder()
                .setBloomFilter(bfBuilder);
    }

    @Override
    public BloomFilter deprotofy(ProtoOperator protoOperator) {
        Summarybucket.ProtoSimpleBloomFilter protoFilter = protoOperator.getBloomFilter();
        return new BloomFilter(
                protoFilter.getNumhashes(),
                // TODO: should technically be something like getBitset().getBitsetBytes().asByteBuffer()
                BitSet.valueOf(protoFilter.getBitset().toByteArray()),
                protoFilter.getFiltersize());
        /*// BitSet is a nested proto message
        BloomFilter bf = new BloomFilter (
                ((Summarybucket.ProtoSimpleBloomFilter.Builder) builder).getNumhashes(),
                BitSet.valueOf(((Summarybucket.ProtoSimpleBloomFilter.Builder) builder).getBitset().toByteArray()),
                ((Summarybucket.ProtoSimpleBloomFilter.Builder) builder).getFiltersize());
        return bf;*/
    }
}
