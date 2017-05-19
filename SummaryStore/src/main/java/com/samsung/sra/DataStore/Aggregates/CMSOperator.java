package com.samsung.sra.DataStore.Aggregates;

import com.clearspring.analytics.stream.frequency.CMSProtofier;
import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.samsung.sra.DataStore.*;
import com.samsung.sra.protocol.SummaryStore.ProtoOperator;
import org.apache.commons.math3.util.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A = CountMinSketch
 * V = Long
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

    private long[] hashA;

    public CMSOperator(int depth, int width, int seed) {
        this.depth = depth;
        this.width = width;
        this.seed = seed;
        initializeHashes();
    }

    /** Copied verbatim from CountMinSketch.java */
    private void initializeHashes() {
        this.hashA = new long[depth];
        Random r = new Random(seed);
        // We're using a linear hash functions
        // of the form (a*x+b) mod p.
        // a,b are chosen independently for each hash function.
        // However we can set b = 0 as all it does is shift the results
        // without compromising their uniformity or independence with
        // the other hashes.
        for (int i = 0; i < depth; ++i) {
            hashA[i] = r.nextInt(Integer.MAX_VALUE);
        }
    }

    @Override
    public CountMinSketch createEmpty() {
        return CMSProtofier.createEmpty(depth, width, hashA);
    }

    @Override
    public CountMinSketch merge(Stream<CountMinSketch> aggrs) {
        return CMSProtofier.merge(aggrs);
    }

    @Override
    public CountMinSketch insert(CountMinSketch aggr, long timestamp, Object val) {
        /*long entry = (long) val[0];
        long count = val.length > 1 ? (long) val[1] : 1;
        aggr.add(entry, count);*/
        aggr.add((long) val, 1);
        return aggr;
    }

    @Override
    public ResultError<Double, Pair<Double, Double>> query(StreamStatistics streamStats,
                                                           Stream<SummaryWindow> summaryWindows,
                                                           Function<SummaryWindow, CountMinSketch> cmsRetriever,
                                                           Stream<LandmarkWindow> landmarkWindows,
                                                           long t0, long t1, Object... params) {
        assert params != null && params.length > 0;
        long targetVal = (long) params[0];
        Function<SummaryWindow, Long> countRetriever = cmsRetriever.andThen(
                cms -> cms.estimateCount(targetVal)
        );
        // FIXME! Returns correct base answer, but we have a better CI construction with a hypergeom distr
        //        (which accounts for, among other things, the fact that we know true count over all values)
        double confidenceLevel = 1;
        if (params.length > 1) {
            confidenceLevel = ((Number) params[1]).doubleValue();
        }
        double sdMultiplier = streamStats.getCVInterarrival();
        return new SumEstimator(t0, t1, summaryWindows, countRetriever, landmarkWindows,
                o -> ((Long) o) == targetVal ? 1L : 0L)
                .estimate(sdMultiplier, confidenceLevel);
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
        return CMSProtofier.deprotofy(protoOperator.getCms(), depth, width, hashA);
    }
}
