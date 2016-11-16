package com.samsung.sra.DataStore.Aggregates;

import com.clearspring.analytics.stream.frequency.CMSProtofier;
import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.samsung.sra.DataStore.Bucket;
import com.samsung.sra.DataStore.ResultError;
import com.samsung.sra.DataStore.StreamStatistics;
import com.samsung.sra.DataStore.WindowOperator;
import com.samsung.sra.protocol.Summarybucket.ProtoOperator;
import org.apache.commons.math3.util.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A = CountMinSketch
 * V = [value to insert: long, count of value (default 1): long]. Count = 1 is a classic CMS
       TODO: change to [byte[], Long]. streamlib already has most of the needed code, just need some wrapper functions
 * R = Long, a count
 * E = Pair<Double, Double>, a CI
 */
public class CMSOperator implements WindowOperator<CountMinSketch,Double,Pair<Double,Double>> {
    private static List<String> supportedQueryTypes = Arrays.asList("frequency", "cms");

    @Override
    public List<String> getSupportedQueryTypes() {
        return supportedQueryTypes;
    }

    private int depth, width, seed;

    public CMSOperator(int depth, int width, int seed) {
        this.depth = depth;
        this.width = width;
        this.seed = seed;
    }

    @Override
    public CountMinSketch createEmpty() {
        return new CountMinSketch(depth, width, seed);
    }

    @Override
    public CountMinSketch merge(Stream<CountMinSketch> aggrs) {
        // TODO: modify CMS code to directly take a stream/iterator w/o buffering into an array first
        CountMinSketch[] all = aggrs.collect(Collectors.toList()).toArray(new CountMinSketch[0]);
        try {
            return CountMinSketch.merge(all);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CountMinSketch insert(CountMinSketch aggr, long timestamp, Object[] val) {
        long entry = (long)val[0];
        long count = val.length > 1 ? (long) val[1] : 1;
        aggr.add(entry, count);
        return aggr;
    }

    @Override
    public ResultError<Double, Pair<Double, Double>> query(StreamStatistics streamStats,
                                                           long T0, long T1, Stream<Bucket> buckets,
                                                           Function<Bucket, CountMinSketch> aggregateRetriever,
                                                           long t0, long t1, Object... params) {
        assert params != null && params.length > 0;
        Function<Bucket, Long> countRetriever = aggregateRetriever.andThen(
                cms -> cms.estimateCount((long) params[0])
        );
        // FIXME! Returns correct base answer, but we have a better CI construction with a hypergeom distr
        //        (which accounts for, among other things, the fact that we know true count over all values)
        SimpleCountOperator.QueryEstimator estimator = new SimpleCountOperator.QueryEstimator(T0, T1, buckets, countRetriever);
        double confidenceLevel = 1;
        if (params.length > 1) {
            confidenceLevel = ((Number) params[1]).doubleValue();
        }
        return estimator.estimate(streamStats, t0, t1, confidenceLevel);
    }

    @Override
    public ResultError<Double, Pair<Double, Double>> getEmptyQueryResult() {
        return new ResultError<>(0d, null);
    }

    /** protofy code needs access to package-local members, so put it in the com.clearspring... package */
    @Override
    public ProtoOperator.Builder protofy(CountMinSketch aggr) {
        return ProtoOperator
                .newBuilder()
                .setCms(CMSProtofier.protofy(aggr));
    }

    @Override
    public CountMinSketch deprotofy(ProtoOperator protoOperator) {
        return CMSProtofier.deprotofy(protoOperator.getCms(), depth, width);
    }
}
