package com.samsung.sra.DataStore.Aggregates;

import com.clearspring.analytics.stream.membership.BFProtofier;
import com.clearspring.analytics.stream.membership.BloomFilter;
import com.samsung.sra.DataStore.*;
import com.samsung.sra.protocol.SummaryStore.ProtoOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *
 * Returns <base answer, probability base answer may be wrong>
 */

//AVRE: Bloomfilter, Long, Boolean, Double
public class BloomFilterOperator implements WindowOperator<BloomFilter, Boolean, Double> {
    private int filterSize; // = 128
    private int nrHashes; // = 7
    private static Logger logger = LoggerFactory.getLogger(BloomFilterOperator.class);

    public BloomFilterOperator(int nrHashes, int filterSize) {
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

            newBloomFilter.addAll(bfItem);
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
    public ResultError<Boolean, Double> query(StreamStatistics streamStatistics, long T0, long T1,
                                              Stream<SummaryWindow> summaryWindows, Function<SummaryWindow, BloomFilter> bloomRetriever, long t0, long t1, Object... params) {
        byte[] value = new byte[Long.SIZE];
        Utilities.longToByteArray((long) params[0],value,0);
        boolean answer = summaryWindows.map(bloomRetriever).map(bf -> bf.isPresent(value)).anyMatch(e -> e);
        if (!answer) { // true negative
            return new ResultError<>(false, 0d);
        } else {
            // FIXME!! Assuming BF is configured with FP rate 0.01
            double pTruePositive = 0.99 * (t1 - t0 + 1d) / (T1 - T0 + 1d);
            return new ResultError<>(true, 1 - pTruePositive);
        }
    }


    /**
     * Return the default answer to a query on an empty aggregate (containing zero elements)
     */
    @Override
    public ResultError<Boolean, Double> getEmptyQueryResult() {
        return new ResultError<>(false, 0d);
    }


    @Override
    public ProtoOperator.Builder protofy(BloomFilter aggr) {
        return BFProtofier.protofy(aggr);
    }

    @Override
    public BloomFilter deprotofy(ProtoOperator protoOperator) {
        return BFProtofier.deprotofy(protoOperator, nrHashes, filterSize);
    }
}
