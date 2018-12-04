/*
* Copyright 2016 Samsung Research America. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.samsung.sra.experiments;

import com.samsung.sra.datastore.ResultError;
import org.apache.commons.math3.util.Pair;

import java.io.Serializable;

/**
 * Statistics over a set of query answer observations. Use {@link Statistics} instead if you want generic statistics
 * over a set of numeric observations.
 *
 * Records error, latency and CI width stats.
 */
public class QueryStatistics implements Serializable {
    private static final int defaultNCDFBins = 10000;
    private final Statistics errorStats, latencyStats, ciWidthStats;
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
        ciWidthStats = new Statistics(requireCDF, nCDFBins);
    }

    public synchronized void addNumericResult(long trueAnswer, ResultError re, double latencyMS) {
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
            {
                double ciWidth = ci.getSecond() - ci.getFirst();
                double ciBase = (ci.getSecond() + ci.getFirst()) / 2d;
                if (ciBase > 1e-6) { // basically epsilon, to avoid getting NaN
                    ciWidthStats.addObservation(ciWidth / ciBase);
                }
            }
        }
    }

    public synchronized void addBooleanResult(boolean trueAnswer, ResultError re, double latencyMS) {
        boolean estimate = (boolean) re.result;
        errorStats.addObservation(estimate ==  trueAnswer ? 0 : 1);
        latencyStats.addObservation(latencyMS / 1000);
        if (re.error instanceof Double) {
            ciWidthStats.addObservation((double) re.error);
        } else { // MAX_THRESH
            assert re.error instanceof Boolean;
            ciWidthStats.addObservation((boolean) re.error ? 0 : 1);
        }
    }

    public synchronized Statistics getErrorStats() {
        return errorStats;
    }

    public synchronized Statistics getLatencyStats() {
        return latencyStats;
    }

    public synchronized Statistics getCIWidthStats() {
        return ciWidthStats;
    }

    public synchronized double getCIMissRate() {
        long N = errorStats.getCount();
        return N > 0 ? (NciMisses / (double) N) : 0;
    }
}
