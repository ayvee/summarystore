package com.samsung.sra.DataStore.Aggregates;

import com.clearspring.analytics.stream.membership.BFProtofier;
import com.clearspring.analytics.stream.membership.BloomFilter;
import com.samsung.sra.DataStore.*;
import com.samsung.sra.protocol.Common.OpType;
import com.samsung.sra.protocol.SummaryStore.ProtoOperator;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableLong;
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
    private static final OpType opType = OpType.BLOOM;

    public BloomFilterOperator(int nrHashes, int filterSize) {
        this.filterSize = filterSize;
        this.nrHashes = nrHashes;
    }

    private static final List<String> supportedQueries = Collections.singletonList("simplebloom");

    @Override
    public OpType getOpType() {
        return opType;
    }

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
    public ResultError<Boolean, Double> query(StreamStatistics streamStatistics,
                                              Stream<SummaryWindow> summaryWindows,
                                              Function<SummaryWindow, BloomFilter> bloomRetriever,
                                              Stream<LandmarkWindow> landmarkWindows,
                                              long t0, long t1, Object... params) {
        long val = (long) params[0];
        boolean inLandmark = landmarkWindows
                .anyMatch(w -> w.values.values().stream()
                        .anyMatch(v -> ((Number) v[0]).longValue() == val));
        if (inLandmark) { // found an explicit match in a landmark
            return new ResultError<>(true, 0d);
        } else {
            byte[] bytes = new byte[Long.SIZE];
            Utilities.longToByteArray((long) params[0], bytes, 0);
            MutableLong T0 = new MutableLong(-1L), T1 = new MutableLong(-1L);
            MutableBoolean answer = new MutableBoolean(false);
            summaryWindows.forEach(window -> {
                if (T0.longValue() != -1L) {
                    T0.setValue(window.tStart);
                }
                T1.setValue(window.tEnd);
                if (answer.isFalse()) {
                    answer.setValue(bloomRetriever.apply(window).isPresent(bytes));
                }
            });
            if (answer.isFalse()) { // true negative
                return new ResultError<>(false, 0d);
            } else {
                // FIXME!! Assuming BF is configured with FP rate 0.01
                double pTruePositive = 0.99 * (t1 - t0 + 1d) / (T1.doubleValue() - T0.doubleValue());
                return new ResultError<>(true, 1 - pTruePositive);
            }
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
