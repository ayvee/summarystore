package com.samsung.sra.DataStore.Aggregates;

import com.samsung.sra.DataStore.*;
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
        private long Cl = -1, Cm = -1, Cr = -1; // count of first bucket, all middle buckets, and last bucket
        private double cv_t = 0; // sigma_t / mu_t

        Estimator(double numSDs) {
            this.numSDs = numSDs;
        }

        Estimator() {
            this(2);
        }

        public ResultError<Double, Pair<Double, Double>> estimate(long t0, long t1) {
            assert ts <= t0 && (tml == -1 || t0 <= tml) && (tmr == -1 || tmr <= t1) && (t1 <= te);
            double ans, sd;
            double Ctot; // total count, i.e. right endpoint of 100% CI
            logger.debug("timestamps = [{}, {}, {}, {}], counts = {}, {}, {}", ts, tml, tmr, te, Cl, Cm, Cr);
            if (tmr == -1) { // only one bucket
                double t = t1 - t0 + 1;
                double T = te - ts + 1;
                logger.debug("t = {}, T = {}", t, T);
                ans = Cl * t/T;
                sd = cv_t * Math.sqrt((Cl * t/T) * t/T * (1 - t/T));
                Ctot = Cl;
            } else {
                double tl = tml-1 - t0 + 1;
                double Tl = tml-1 - ts + 1;
                double tr = t1 - tmr + 1;
                double Tr = te - tmr + 1;
                logger.debug("tl = {}, Tl = {}, tr = {}, Tr = {}", tl, Tl, tr, Tr);
                ans = Cl * tl/Tl + Cm + Cr * tr/Tr;
                sd = cv_t * Math.sqrt(
                        (Cl * tl/Tl) * tl/Tl * (1 - tl/Tl) +
                        (Cr * tr/Tr) * tr/Tr * (1 - tr/Tr));
                Ctot = Cl + Cm + Cr;
            }
            double CIl = Math.max(0d, ans - numSDs * sd);
            double CIr = Math.min(Ctot, ans + numSDs * sd);
            return new ResultError<>(ans, new Pair<>(CIl, CIr));
        }
    }

    @Override
    public ResultError<Double, Pair<Double, Double>> query(StreamStatistics streamStats, long T0, long T1, Stream<Bucket> buckets, Function<Bucket, Long> countRetriever, long t0, long t1, Object... params) {
        Estimator estimator = new Estimator();
        estimator.ts = T0;
        estimator.te = T1;
        buckets.forEach(b -> {
            if (estimator.Cl == -1) { // first bucket
                assert estimator.ts == b.tStart;
                estimator.Cl = countRetriever.apply(b);
            } else if (estimator.Cm == -1) { // second bucket
                estimator.tml = b.tStart;
                estimator.Cm = estimator.Cr = countRetriever.apply(b);
                estimator.tmr = b.tStart;
            } else { // third or later bucket
                estimator.tmr = b.tStart;
                estimator.Cr = countRetriever.apply(b);
                estimator.Cm += estimator.Cr;
            }
        });
        if (estimator.tml == estimator.tmr) { // there were <= 2 buckets, i.e. there were no "middle" buckets
            estimator.Cm = 0;
        } else {
            // the loop above set estimator.Cm = count of all buckets starting from 2nd including last; subtract last now
            estimator.Cm -= estimator.Cr;
        }
        estimator.cv_t = streamStats.getCVInterarrival();
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
