package com.samsung.sra.DataStore.Aggregates;

import com.samsung.sra.DataStore.*;
import com.samsung.sra.protocol.SummaryStore.ProtoOperator;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class SimpleCountOperator implements WindowOperator<Long,Double,Pair<Double,Double>> {
    private static Logger logger = LoggerFactory.getLogger(SimpleCountOperator.class);

    private static final List<String> supportedQueries = Collections.singletonList("count");

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
        return aggr + 1;
    }

    /**
     * Statistical estimation logic for a single query.
     *
     * NOTE: if you're copying this code as a template, be aware it is more complex than it needs to be. You may be
     *       able to write a simpler impl from scratch.
     *
     * Details: code is more general than it needs to be. It can answer any query contained in [T0, T1], not just the
     * particular [t0, t1] posed by the user. (An older version, now in git history, could even handle queries not
     * contained in [T0, T1].) This was meant to plug into the "fuzzy cache" we were talking about, but we're not
     * getting around to that any time soon.
     */
    static class QueryEstimator {
        private long ts = -1; // start timestamp of first bucket
        private long tml = -1; // 1 + end timestamp of first bucket (= start timestamp of 2nd bucket if there is > 1 bucket)
        private long tmr = -1; // start timestamp of last bucket (if there is > 1 bucket)
        private long te = -1; // end timestamp of last bucket
        private long Cl = -1; // count of first bucket
        private long Cm = -1; // count of all middle buckets (if there are > 2 buckets)
        private long Cr = -1; // count of last bucket (if there is > 1 bucket)

        QueryEstimator(long T0, long T1, Stream<SummaryWindow> buckets, Function<SummaryWindow, Long> countRetriever) {
            MutableLong numBuckets = new MutableLong(0L); // not a plain long because of Java Stream limitations
            buckets.forEach(b -> {
                numBuckets.increment();
                if (numBuckets.toLong() == 1) { // first bucket
                    assert b.tStart == T0;
                    ts = T0;
                    Cl = countRetriever.apply(b);
                } else if (numBuckets.toLong() == 2) { // second bucket
                    tml = b.tStart;
                    Cm = Cr = countRetriever.apply(b);
                    tmr = b.tStart;
                } else { // third or later bucket
                    tmr = b.tStart;
                    Cr = countRetriever.apply(b);
                    Cm += Cr;
                }
            });
            assert numBuckets.toLong() > 0;
            if (numBuckets.toLong() == 1) { // no middle or right buckets
                tml = T1+1;
                te = T1;
                Cm = -1;
                Cr = -1;
            } else if (numBuckets.toLong() == 2) { // no middle bucket
                te = T1;
                Cm = -1;
            } else {
                te = T1;
                // the loop above set estimator.Cm = count of all buckets starting from 2nd including last; subtract last now
                Cm -= Cr;
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
                                                long C, long T0, long T1, long t0, long t1) {
            long overlap = overlap(T0, T1, t0, t1);
            if (C != -1 && overlap != 0) {
                logger.trace("conditional estimate: C = {}, [T0, T1] = [{}, {}], [t0, t1] = [{}, {}], overlap = {}",
                        C, T0, T1, t0, t1, overlap);
                long length = length(T0, T1);
                if (overlap == length) lowerbound.add(C);
                upperbound.add(C);
                double ratio = overlap / (double)length;
                mean.add(C * ratio);
                var.add(C * ratio * (1 - ratio));
            }
        }

        public ResultError<Double, Pair<Double, Double>> estimate(
                StreamStatistics streamStats, long t0, long t1, double confidenceLevel) {
            assert ts <= t0 && t0 <= t1 && t1 <= te;
            // Check overlap with each of these intervals:
            //     [ts, tml-1], [tml, tmr-1], [tmr, te]
            // and do a proportional count on each
            logger.trace("timestamps = [{}, {}, {}, {}], counts = ({}, {}, {})", ts, tml, tmr, te, Cl, Cm, Cr);
            MutableDouble
                    mean = new MutableDouble(0), var = new MutableDouble(0),
                    lowerbound = new MutableDouble(0), upperbound = new MutableDouble(0);
            conditionalEstimate(mean, var, lowerbound, upperbound, Cl, ts, tml-1, t0, t1);
            conditionalEstimate(mean, var, lowerbound, upperbound, Cm, tml, tmr-1, t0, t1);
            conditionalEstimate(mean, var, lowerbound, upperbound, Cr, tmr, te, t0, t1);

            double ans = mean.toDouble();
            double CIl, CIr;
            double numSDs = Utilities.getNormalQuantile((1 + confidenceLevel) / 2d); // ~150 nanosecs, not a bottleneck
            if (Double.isInfinite(numSDs)) { // return 100% CI
                CIl = lowerbound.toDouble();
                CIr = upperbound.toDouble();
            } else {
                double sd = streamStats.getCVInterarrival() * Math.sqrt(var.toDouble());
                CIl = Math.max(ans - numSDs * sd, lowerbound.toDouble());
                CIr = Math.min(ans + numSDs * sd, upperbound.toDouble());
            }
            return new ResultError<>(ans, new Pair<>(CIl, CIr));
        }
    }

    @Override
    public ResultError<Double, Pair<Double, Double>> query(StreamStatistics streamStats,
                                                           long T0, long T1,
                                                           Stream<SummaryWindow> summaryWindows, Function<SummaryWindow, Long> countRetriever,
                                                           long t0, long t1, Object... params) {
        QueryEstimator estimator = new QueryEstimator(T0, T1, summaryWindows, countRetriever);
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
