package com.samsung.sra.DataStore.Aggregates;

import com.samsung.sra.DataStore.*;
import com.samsung.sra.protocol.Summarybucket.ProtoOperator;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/** TODO?: return confidence level along with each CI? */
public class SumOperator implements WindowOperator<Long,Double,Pair<Double,Double>> {
    private static Logger logger = LoggerFactory.getLogger(SumOperator.class);

    private static final List<String> supportedQueries = Collections.singletonList("sum");

    @Override
    public List<String> getSupportedQueryTypes() {
        return supportedQueries;
    }

    @Override
    public Long createEmpty() {
        return 0L;
    }

    @Override
    public Long merge(Stream<Long> aggrs) {
        return aggrs.mapToLong(Long::longValue).sum();
    }

    @Override
    public Long insert(Long aggr, long ts, Object[] val) {
        return aggr + (long) val[0];
    }

    /**
     * Statistical estimation logic for a single query.
     *
     * Same comments as {@link SimpleCountOperator.QueryEstimator}, code is more general than it needs to be.
     */
    static class QueryEstimator {
        private long ts = -1; // start timestamp of first bucket
        private long tml = -1; // 1 + end timestamp of first bucket (= start timestamp of 2nd bucket if there is > 1 bucket)
        private long tmr = -1; // start timestamp of last bucket (if there is > 1 bucket)
        private long te = -1; // end timestamp of last bucket
        private long Sl = -1; // count of first bucket
        private long Sm = -1; // count of all middle buckets (if there are > 2 buckets)
        private long Sr = -1; // count of last bucket (if there is > 1 bucket)

        QueryEstimator(long T0, long T1, Stream<Bucket> buckets, Function<Bucket, Long> sumRetriever) {
            MutableLong numBuckets = new MutableLong(0L); // not a plain long because of Java Stream limitations
            buckets.forEach(b -> {
                numBuckets.increment();
                if (numBuckets.toLong() == 1) { // first bucket
                    assert b.tStart == T0;
                    ts = T0;
                    Sl = sumRetriever.apply(b);
                } else if (numBuckets.toLong() == 2) { // second bucket
                    tml = b.tStart;
                    Sm = Sr = sumRetriever.apply(b);
                    tmr = b.tStart;
                } else { // third or later bucket
                    tmr = b.tStart;
                    Sr = sumRetriever.apply(b);
                    Sm += Sr;
                }
            });
            assert numBuckets.toLong() > 0;
            if (numBuckets.toLong() == 1) { // no middle or right buckets
                tml = T1+1;
                te = T1;
                Sm = -1;
                Sr = -1;
            } else if (numBuckets.toLong() == 2) { // no middle bucket
                te = T1;
                Sm = -1;
            } else {
                te = T1;
                // the loop above set estimator.Sm = count of all buckets starting from 2nd including last; subtract last now
                Sm -= Sr;
            }
        }

        /** What is the length of [l, r]? */
        private static long length(long l, long r) {
            return r - l + 1;
        }

        /** How much do [a, b] and [p, q] overlap? */
        private static long overlap(long a, long b, long p, long q) {
            return Math.max(Math.min(q, b) - Math.max(a, p) + 1, 0);
        }

        /**
         * Given count[T0, T1] = C, update mean, 1/cv_t * variance and 100% confidence bounds
         * for our distribution on count(portion of[t0, t1] that intersects [T0, T1])
         */
        private static void conditionalEstimate(MutableDouble mean, MutableDouble var,
                                                MutableDouble lowerbound, MutableDouble upperbound,
                                                long S, long T0, long T1, long t0, long t1) {
            long overlap = overlap(T0, T1, t0, t1);
            if (S != -1 && overlap != 0) {
                long length = length(T0, T1);
                logger.trace("conditional estimate: S = {}, [T0, T1] = [{}, {}], [t0, t1] = [{}, {}], overlap / length = {} / {}",
                        S, T0, T1, t0, t1, overlap, length);
                if (overlap == length) lowerbound.add(S);
                upperbound.add(S);
                double ratio = overlap / (double)length;
                mean.add(S * ratio);
                var.add(S * ratio * (1 - ratio));
            }
        }

        public ResultError<Double, Pair<Double, Double>> estimate(
                StreamStatistics streamStats, long t0, long t1, double confidenceLevel) {
            assert ts <= t0 && t0 <= t1 && t1 <= te;
            // Check overlap with each of these intervals:
            //     [ts, tml-1], [tml, tmr-1], [tmr, te]
            // and do a proportional sum on each
            logger.trace("timestamps = [{}, {}, {}, {}], sums = ({}, {}, {})", ts, tml, tmr, te, Sl, Sm, Sr);
            MutableDouble
                    mean = new MutableDouble(0), var = new MutableDouble(0),
                    lowerbound = new MutableDouble(0), upperbound = new MutableDouble(0);
            conditionalEstimate(mean, var, lowerbound, upperbound, Sl, ts, tml-1, t0, t1);
            conditionalEstimate(mean, var, lowerbound, upperbound, Sm, tml, tmr-1, t0, t1);
            conditionalEstimate(mean, var, lowerbound, upperbound, Sr, tmr, te, t0, t1);

            double ans = mean.toDouble();
            double CIl, CIr;
            double numSDs = Utilities.getNormalQuantile((1 + confidenceLevel) / 2d); // ~150 nanosecs, not a bottleneck
            if (Double.isInfinite(numSDs)) { // return 100% CI
                CIl = lowerbound.toDouble();
                CIr = upperbound.toDouble();
            } else {
                double cv_t = streamStats.getCVInterarrival(), cv_v = streamStats.getCVValue();
                double mu_v = streamStats.getMeanValue();
                double sd = Math.sqrt((cv_t * cv_t + cv_v * cv_v) * Math.sqrt(mu_v * var.toDouble()));
                logger.trace("cv_t = {}, cv_v = {}, mu_v = {}, var = {}, sd = {}",
                        cv_t, cv_v, mu_v, var, sd);
                CIl = Math.max(ans - numSDs * sd, lowerbound.toDouble());
                CIr = Math.min(ans + numSDs * sd, upperbound.toDouble());
            }
            return new ResultError<>(ans, new Pair<>(CIl, CIr));
        }
    }

    @Override
    public ResultError<Double, Pair<Double, Double>> query(StreamStatistics streamStats,
                                    long T0, long T1, Stream<Bucket> buckets, Function<Bucket, Long> sumRetriever,
                                    long t0, long t1, Object... params) {
        QueryEstimator estimator = new QueryEstimator(T0, T1, buckets, sumRetriever );
        double confidenceLevel = 1;
        if (params != null && params.length > 0) {
            confidenceLevel = ((Number) params[0]).doubleValue();
        }
        return estimator.estimate(streamStats, t0, t1, confidenceLevel);
    }

    @Override
    public ResultError<Double, Pair<Double, Double>> getEmptyQueryResult() {
        return new ResultError<>(0d, new Pair<>(0d, 0d));
    }

    @Override
    public ProtoOperator.Builder protofy(Long aggr) {
        return ProtoOperator
                .newBuilder()
                .setLong(aggr);
    }

    @Override
    public Long deprotofy(ProtoOperator operator) {
        return operator.getLong();
    }
}
