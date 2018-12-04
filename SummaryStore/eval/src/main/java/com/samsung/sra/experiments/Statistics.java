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

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Collectors;

/**
 * Min/max/avg/CDF over a set of numeric observations. Use the specialized {@link QueryStatistics} instead if the
 * observations are measurements of query error.
 */
public class Statistics implements Serializable {
    private static final int defaultNCDFBins = 10000;
    private long N = 0;
    private double sum = 0, sqsum = 0;
    private double min = Double.MAX_VALUE, max = Double.MIN_VALUE;
    private CDF cdf = null;

    public Statistics() {
        this(false);
    }

    public Statistics(boolean requireCDF) {
        this(requireCDF, defaultNCDFBins);
    }

    public Statistics(boolean requireCDF, int nCDFBins) {
        if (requireCDF) {
            cdf = new CDF(nCDFBins);
        }
    }

    /** Weighted mixture statistics */
    public Statistics(Collection<Statistics> stats, Collection<Double> weights) {
        assert stats != null && weights != null && !stats.isEmpty() && stats.size() == weights.size();
        boolean haveCDF = true;
        double totalWeight = weights.stream().mapToDouble(Double::doubleValue).sum();
        for (Iterator statsi = stats.iterator(), weighti = weights.iterator(); statsi.hasNext() && weighti.hasNext();) {
            Statistics stat = (Statistics)statsi.next();
            double probability = (Double)weighti.next() / totalWeight;
            N += stat.N;
            sum += probability * stat.sum;
            sqsum += probability * stat.sqsum;
            min = Math.min(min, stat.min);
            max = Math.max(max, stat.max);
            haveCDF = haveCDF && stat.cdf != null;
        }
        if (haveCDF) {
            cdf = new CDF(defaultNCDFBins, stats.stream().map(s -> s.cdf).collect(Collectors.toList()), weights);
        }
    }

    public synchronized void addObservation(double obs) {
        ++N;
        sum += obs;
        sqsum += obs * obs;
        min = Math.min(min, obs);
        max = Math.max(max, obs);
        if (cdf != null) cdf.addValue(obs);
    }

    public long getCount() {
        return N;
    }

    public synchronized double getMean() {
        return N > 0 ? sum / N : 0;
    }

    public synchronized double getStandardDeviation() {
        return N > 1 ? Math.sqrt((sqsum - sum / N) / (N-1)) : 0;
    }

    /** Return P(X <= x) */
    public synchronized double getCumulativeDensity(double x) {
        assert cdf != null;
        return cdf.getCumulativeDensity(x);
    }

    /** Quantile/ICDF. Return x such that P(X <= x) = Q */
    public synchronized Double getQuantile(double Q) {
        assert cdf != null;
        return cdf.getQuantile(Q);
    }

    // forces Java to not use scientific notation
    /*private static DecimalFormat doubleFormat = new DecimalFormat("#");
    static {
        doubleFormat.setMaximumFractionDigits(340);
    }*/
    private String format(double d) {
        //return Double.isNaN(d) ? "NaN" : doubleFormat.format(d);
        //return Double.toString(d);
        return String.format("%.5f", d);
    }

    public synchronized String getErrorbars() {
        return getMean() + ":" + getStandardDeviation();
    }

    public synchronized void writeCDF(String filename) throws IOException {
        assert cdf != null;
        cdf.writeCDF(filename);
    }
}
