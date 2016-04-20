package com.samsung.sra.DataStoreExperiments;

import org.apache.commons.math3.analysis.solvers.BrentSolver;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Statistics implements Serializable {
    private SummaryStatistics sstats = null;
    private DescriptiveStatistics dstats = null;

    public Statistics(boolean requireCDF) {
        if (requireCDF) {
            dstats = new DescriptiveStatistics();
        } else {
            sstats = new SummaryStatistics();
        }
    }

    public synchronized void addObservation(double obs) {
        if (dstats != null) {
            dstats.addValue(obs);
        } else {
            sstats.addValue(obs);
        }
    }

    public synchronized double getMean() {
        if (dstats != null) {
            return dstats.getMean();
        } else {
            return sstats.getMean();
        }
    }

    public synchronized double getStandardDeviation() {
        if (dstats != null) {
            return dstats.getStandardDeviation();
        } else {
            return sstats.getStandardDeviation();
        }
    }

    /** Return P(X <= x) */
    public synchronized double getCDF(double x) {
        assert dstats != null;
        // FIXME: hack because Apache commons doesn't have a CDF function
        return (new BrentSolver()).solve(1000, Q -> getQuantile(Q) - x, 1e-6, 1);
    }

    /** Quantile/ICDF. Return x such that P(X <= x) = Q */
    public synchronized Double getQuantile(double Q) {
        assert dstats != null;
        return dstats.getPercentile(Q * 100);
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
        if (dstats != null) {
            return format(dstats.getMin()) + ":" + format(dstats.getPercentile(25)) + ":" +
                    format(dstats.getPercentile(50)) + ":" +
                    format(dstats.getPercentile(75)) + ":" + format(dstats.getMax()) + "_" +
                    format(dstats.getMean()) + ":" + format(dstats.getStandardDeviation());
        } else {
            return sstats.getMean() + ":" + sstats.getStandardDeviation();
        }
    }

    public synchronized void writeCDF(String filename) throws IOException {
        assert dstats != null;
        try (BufferedWriter br = Files.newBufferedWriter(Paths.get(filename))) {
            br.write("#" + getErrorbars() + "\n");
            br.write(dstats.getMin() + "\t0" + "\n");
            for (int i = 1; i <= 100; ++i) {
                br.write(dstats.getPercentile(i) + "\t" + (i / 100d) + "\n");
            }
        }
    }
}
