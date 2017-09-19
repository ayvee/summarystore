package com.samsung.sra.datastore.aggregates;

import com.samsung.sra.datastore.LandmarkWindow;
import com.samsung.sra.datastore.ResultError;
import com.samsung.sra.datastore.SummaryWindow;
import com.samsung.sra.datastore.Utilities;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Statistical estimation logic for a single sum/count query. Returns (ans, CI) for [t0, t1] given counts/sums for a
 * list of decayed windows completely covering the time range. (And optionally also overlapping landmark windows.)
 *
 * An older version, now in git history, was also capable of answering queries for [t0, t1] not completely covered by
 * the given decayed windows. This was meant to plug into the "fuzzy cache" we thought about building.
 */
class SumEstimator {
    private static final Logger logger = LoggerFactory.getLogger(SumEstimator.class);

    private final long t0, t1;

    private long nDecayedWindows;
    private long ts = -1; // start timestamp of first decayed window
    private long tm0 = -1; // 1 + end timestamp of first decayed window
                           // (= start timestamp of 2nd decayed window window if there is > 1 decayed window)
    private long tm1 = -1; // start timestamp of last decayed window (if there is > 1 window)
    private long te = -1; // end timestamp of last decayed window

    private long Sl = -1; // sum of first decayed window
    private long Sm = -1; // sum of all middle decayed windows (if there are > 2 windows)
    private long Sr = -1; // sum of last decayed window (if there is > 1 window)
    private long tl = 0; // overlap([t0, t1], first decayed window minus all overlapping landmark windows)
    private long tr = 0; // ditto if there is more than one decayed window
    private long Tl = 0; // total_length(first decayed window minus all overlapping landmark windows)
    private long Tr = 0; // ditto if there is more than one decayed window

    private long Slandmark = 0; // sum of all overlapping landmark values

    private double mean = 0, var = 0;
    private long lowerbound = 0, upperbound = 0;

    SumEstimator(long t0, long t1,
                 Stream<SummaryWindow> decayedWindows, Function<SummaryWindow, Long> aggrRetriever,
                 Stream<LandmarkWindow> landmarkWindows, Function<Object, Long> rawValueParser) {
        this.t0 = t0;
        this.t1 = t1;
        processDecayedWindows(decayedWindows, aggrRetriever);
        processLandmarkWindows(landmarkWindows, rawValueParser);
        constructEstimate();
        logger.debug("[t0, tm0, tm1, te] = [{}, {}, {}, {}]; tl / Tl * Sl = {} / {} * {}, Sm = {}, " +
                "tr / Tr * Sr = {} / {} * {}; Slandmark = {}",
                t0, tm0, tm1, te, tl, Tl, Sl, Sm, tr, Tr, Sr, Slandmark);
    }

    private void processDecayedWindows(Stream<SummaryWindow> windows, Function<SummaryWindow, Long> sumRetriever) {
        MutableLong numWindows = new MutableLong(0L); // not a plain long because of Java Stream limitations
        MutableLong lastTEnd = new MutableLong();
        windows.forEach(w -> {
            numWindows.increment();
            if (numWindows.toLong() == 1) { // first window
                ts = w.ts;
                Sl = sumRetriever.apply(w);
            } else if (numWindows.toLong() == 2) { // second window
                tm0 = w.ts;
                Sm = Sr = sumRetriever.apply(w);
                tm1 = w.ts;
            } else { // third or later window
                tm1 = w.ts;
                Sr = sumRetriever.apply(w);
                Sm += Sr;
            }
            lastTEnd.setValue(w.te);
        });
        nDecayedWindows = numWindows.toLong();
        assert nDecayedWindows > 0;

        long tEnd = lastTEnd.longValue();
        if (nDecayedWindows == 1) { // no middle or right buckets
            tm0 = tEnd + 1;
            te = tEnd;
            Sm = -1;
            Sr = -1;
        } else if (nDecayedWindows == 2) { // no middle bucket
            te = tEnd;
            Sm = -1;
        } else {
            te = tEnd;
            // the loop above set estimator.Sm = count of all buckets starting from 2nd including last; subtract last now
            Sm -= Sr;
        }

        Tl = length(ts, tm0-1);
        tl = overlap(ts, tm0-1, t0, t1);
        if (nDecayedWindows > 1) {
            Tr = length(tm1, te);
            tr = overlap(tm1, te, t0, t1);
        }
    }

    private void processLandmarkWindows(Stream<LandmarkWindow> windows, Function<Object, Long> valueParser) {
        windows.forEach(w -> {
            Tl -= overlap(w.ts, w.te, ts, tm0-1);
            tl -= overlap(w.ts, w.te, t0, tm0-1);
            if (nDecayedWindows > 1) {
                Tr -= overlap(w.ts, w.te, tm1, te);
                tr -= overlap(w.ts, w.te, tm1, t1);
            }

            w.values.forEach((t, v) -> {
                if (t0 <= t && t <= t1) {
                    Slandmark += valueParser.apply(v);
                }
            });
        });
    }

    private void constructEstimate() {
        updateEstimate(Slandmark, 1, 1);
        updateEstimate(Sl, tl, Tl);
        if (nDecayedWindows > 1) {
            updateEstimate(Sr, tr, Tr);
        }
        if (nDecayedWindows > 2) {
            updateEstimate(Sm, 1, 1);
        }
    }

    private void updateEstimate(long S, long t, long T) {
        upperbound += S;
        if (t == T) lowerbound += S;
        if (T > 0) {
            double ratio = t / (double) T;
            mean += S * ratio;
            var += S * ratio * (1 - ratio);
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

    public ResultError<Double, Pair<Double, Double>> estimate(double sdMultiplier, double confidenceLevel) {
        double ans = mean;
        double CIl, CIr;
        double numSDs = Utilities.getNormalQuantile((1 + confidenceLevel) / 2d); // ~150 nanosecs, not a bottleneck
        if (Double.isInfinite(numSDs)) { // return 100% CI
            CIl = lowerbound;
            CIr = upperbound;
        } else {
            /*double cv_t = streamStats.getCVInterarrival(), cv_v = streamStats.getCVValue();
            double mu_v = streamStats.getMeanValue();
            double sd = Math.sqrt((cv_t * cv_t + cv_v * cv_v) * Math.sqrt(mu_v * var.toDouble()));
            logger.trace("cv_t = {}, cv_v = {}, mu_v = {}, var = {}, sd = {}",
                    cv_t, cv_v, mu_v, var, sd);*/
            double sd = sdMultiplier * Math.sqrt(var);
            CIl = Math.max(ans - numSDs * sd, lowerbound);
            CIr = Math.min(ans + numSDs * sd, upperbound);
        }
        return new ResultError<>(ans, new Pair<>(CIl, CIr));
    }
}
