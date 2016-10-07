package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.ResultError;
import org.apache.commons.math3.util.Pair;

import java.io.Serializable;

/**
 * Statistics over a set of query answer observations. Use {@link Statistics} instead if you want generic statistics
 * over a set of numeric observations.
 *
 * Records
 * 1. The distribution over percentage error in query answers
 * 2. CI miss rate: what fraction of query answers fell outside the CI bound?
 * 3. TODO: some measure of CI width
 */
public class QueryStatistics implements Serializable {
    private static final int defaultNCDFBins = 10000;
    private final Statistics errorStats, latencyStats;
    private long NciMisses = 0; // # of queries where CI did not include true answer (\in [0, errorStats.N])

    public QueryStatistics() {
        this(true);
    }

    public QueryStatistics(boolean requireCDF) {
        this(requireCDF, defaultNCDFBins);
    }

    public QueryStatistics(boolean requireCDF, int nCDFBins) {
        errorStats = new Statistics(requireCDF, nCDFBins);
        latencyStats = new Statistics(requireCDF, nCDFBins);
    }

    public synchronized void addResult(long trueAnswer, ResultError re, double latencyMS) {
        double estimate = ((Number) re.result).doubleValue();
        double error = Math.abs(estimate - trueAnswer) / (1d + trueAnswer);
        errorStats.addObservation(error);
        latencyStats.addObservation(latencyMS / 1000);

        if (re.error != null) { // NOTE: silently ignores queries that don't compute CIs
            assert re.error instanceof Pair;
            Pair<Double, Double> ci = (Pair<Double, Double>) re.error;
            if (estimate < ci.getFirst() || estimate > ci.getSecond()) {
                ++NciMisses;
            }
        }
    }

    public synchronized Statistics getErrorStats() {
        return errorStats;
    }

    public synchronized Statistics getLatencyStats() {
        return latencyStats;
    }

    public synchronized double getCIMissRate() {
        long N = errorStats.getCount();
        return N > 0 ? (NciMisses / (double) N) : 0;
    }
}