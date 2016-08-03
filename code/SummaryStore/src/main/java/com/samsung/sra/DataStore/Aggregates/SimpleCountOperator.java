package com.samsung.sra.DataStore.Aggregates;

import com.samsung.sra.DataStore.*;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/** TODO?: return confidence level along with each CI */
public class SimpleCountOperator implements WindowOperator<Long, Long, Double, Pair<Double, Double>> {
    private static Logger logger = LoggerFactory.getLogger(SimpleCountOperator.class);

    private static final List<String> supportedQueries = Collections.singletonList("count");

    /*public enum EstimationAlgo {
        UPPER_BOUND {
            @Override
            long estimate(long qt0, long qt1, long bt0, long bt1, long bCount) {
                return bCount;
            }
        },
        PROPORTIONAL {
            @Override
            long estimate(long qt0, long qt1, long bt0, long bt1, long bCount) {
                // the intersection of [a, b] and [p, q] is [max(a, p), min(b, q)]
                long l = Math.max(qt0, bt0), r = Math.min(qt1, bt1);
                assert r >= l && r - l <= bt1 - bt0;
                return (long)((double)bCount * (r - l + 1d) / (bt1 - bt0 + 1d));
            }
        },
        HALF_BOUND {
            @Override
            long estimate(long qt0, long qt1, long bt0, long bt1, long bCount) {
                if (qt0 <= bt0 && bt1 <= qt1) { // perfect alignment, meaning bCount is the true answer
                    return bCount;
                } else {
                    return bCount / 2;
                }
            }
        };

        abstract long estimate(long qt0, long qt1, long bt0, long bt1, long bCount);
    }*/

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
    public Long insert(Long aggr, long ts, Long val) {
        return aggr + 1;
    }

    static class Estimator {
        // Number of standard deviations in CI.  We make a normality assumption, so
        // e.g. numSDs == 2 means 95% CI
        final double numSDs;

        private long ts = -1; // start timestamp of first bucket
        private long tml = -1; // start timestamp of second bucket
        private long tmr = -1; // start timestamp of last bucket (if there is > 1 bucket)
        private long te = -1; // end timestamp of last bucket
        private long Cl = -1; // count of first bucket
        private long Cm = -1; // count of all middle buckets (if there are > 2 buckets)
        private long Cr = -1; // count of last bucket (if there is > 1 bucket)
        private double sigma_t = 0, mu_t = 0; // mean and variance of interarrival time

        Estimator(double numSDs) {
            this.numSDs = numSDs;
        }

        Estimator() {
            this(2);
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
         * Given count[T0, T1] = C, update mean and 1/cv_t * variance for
         * count(portion of[t0, t1] that intersects [T0, T1])
         */
        private static void conditionalEstimate(MutableDouble mean, MutableDouble var,
                                                long C, long T0, long T1, long t0, long t1) {
            long overlap = overlap(T0, T1, t0, t1);
            if (C != -1 && overlap != 0) {
                logger.debug("conditional estimate: C = {}, [T0, T1] = [{}, {}], [t0, t1] = [{}, {}], overlap = {}",
                        C, T0, T1, t0, t1, overlap);
                double ratio = (double)overlap / length(T0, T1);
                mean.add(C * ratio);
                var.add(C * ratio * (1 - ratio));
            }
        }

        private static void unconditionalEstimate(MutableDouble mean, MutableDouble var, long overlap, double mu_t) {
            assert overlap >= 0;
            if (overlap > 0) {
                logger.debug("unconditional estimate: overlap = {}", overlap);
                mean.add(overlap / mu_t);
                var.add(overlap / mu_t);
            }
        }

        public ResultError<Double, Pair<Double, Double>> estimate(long t0, long t1) {
            // Check overlap with each of these intervals:
            //     (-inf, ts-1], [ts, tml-1], [tml, tmr-1], [tmr, te], [te+1, inf)
            // Middle three intervals: we know counts, do a conditional estimate (proportional count)
            // First and last intervals: do an unconditional estimate (T / mu)
            logger.debug("timestamps = [{}, {}, {}, {}], counts = ({}, {}, {})", ts, tml, tmr, te, Cl, Cm, Cr);
            MutableDouble mean = new MutableDouble(0), var = new MutableDouble(0);
            // FIXME: should we use C/T instead of long-term average mu_t for unconditional?
            unconditionalEstimate(mean, var, overlap(t0, t1, Long.MIN_VALUE, ts-1), mu_t);
            conditionalEstimate(mean, var, Cl, ts, tml-1, t0, t1);
            conditionalEstimate(mean, var, Cm, tml, tmr-1, t0, t1);
            conditionalEstimate(mean, var, Cr, tmr, te, t0, t1);
            unconditionalEstimate(mean, var, overlap(t0, t1, te+1, Long.MAX_VALUE), mu_t);
            double ans = mean.toDouble(), sd = sigma_t / mu_t * Math.sqrt(var.toDouble());
            double CIl = Math.max(ans - numSDs * sd, 0d), CIr = ans + numSDs * sd;
            if (ts <= t0) {
                if (t1 <= tml) {
                    CIr = Math.min(CIr, Cl);
                } else if (Cm != -1 && t1 <= tmr) {
                    CIr = Math.min(CIr, Cl+Cm);
                } else if (t1 <= te) {
                    CIr = Math.max(CIr, Cl+Cm+Cr);
                }
            }
            return new ResultError<>(ans, new Pair<>(CIl, CIr));
        }
    }

    @Override
    public ResultError<Double, Pair<Double, Double>> query(StreamStatistics streamStats, long T0, long T1, Stream<Bucket> buckets, Function<Bucket, Long> countRetriever, long t0, long t1, Object... params) {
        Estimator estimator = new Estimator();
        MutableLong numBuckets = new MutableLong(0L); // not a plain long because of Java Stream limitations
        buckets.forEach(b -> {
            numBuckets.increment();
            if (numBuckets.toLong() == 1) { // first bucket
                assert b.tStart == T0;
                estimator.ts = T0;
                estimator.Cl = countRetriever.apply(b);
            } else if (numBuckets.toLong() == 2) { // second bucket
                estimator.tml = b.tStart;
                estimator.Cm = estimator.Cr = countRetriever.apply(b);
                estimator.tmr = b.tStart;
            } else { // third or later bucket
                estimator.tmr = b.tStart;
                estimator.Cr = countRetriever.apply(b);
                estimator.Cm += estimator.Cr;
            }
        });
        assert numBuckets.toLong() > 0;
        if (numBuckets.toLong() == 1) { // no middle or right buckets
            estimator.tml = T1+1;
            estimator.te = T1;
            estimator.Cm = -1;
            estimator.Cr = -1;
        } else if (numBuckets.toLong() == 2) { // no middle bucket
            estimator.te = T1;
            estimator.Cm = -1;
        } else {
            estimator.te = T1;
            // the loop above set estimator.Cm = count of all buckets starting from 2nd including last; subtract last now
            estimator.Cm -= estimator.Cr;
        }
        estimator.mu_t = streamStats.getMeanInterarrival();
        estimator.sigma_t = streamStats.getSDInterarrival();
        return estimator.estimate(t0, t1);
    }

    /*@Override
    public ResultError<Long, Long> query(StreamStatistics streamStats, Stream<Bucket> buckets, Function<Bucket, Long> countRetriever, long t0, long t1, Object... params) {
        return new ResultError<>(buckets.map(b ->
                estimationAlgo.estimate(t0, t1, b.tStart, b.tEnd, countRetriever.apply(b))
        ).mapToLong(Long::longValue).sum(), 0L);
    }*/

    @Override
    public ResultError<Double, Pair<Double, Double>> getEmptyQueryResult() {
        return new ResultError<>(0d, new Pair<>(0d, 0d));
    }

    @Override
    public int getBytecount() {
        return 8;
    }

    @Override
    public void serialize(Long aggr, byte[] array, int startIndex) {
        Utilities.longToByteArray(aggr, array, startIndex);
    }

    @Override
    public Long deserialize(byte[] array, int startIndex) {
        return Utilities.byteArrayToLong(array, startIndex);
    }
}
