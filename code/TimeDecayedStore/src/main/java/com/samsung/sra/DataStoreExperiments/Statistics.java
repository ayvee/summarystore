package com.samsung.sra.DataStoreExperiments;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;

public class Statistics {
    /*private double sum = 0, sqsum = 0, N = 0;
    private double min = Double.MAX_VALUE, max = Double.MIN_VALUE;*/
    private SummaryStatistics sstats = null;
    private DescriptiveStatistics dstats = null;

    public Statistics(boolean requirePercentile) {
        if (requirePercentile) {
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
        /*sum += obs;
        sqsum += obs * obs;
        N += 1;
        min = Math.min(min, obs);
        max = Math.max(max, obs);*/
    }

    public synchronized double getAverage() {
        if (dstats != null) {
            return dstats.getMean();
        } else {
            return sstats.getMean();
        }
        /*if (N > 0) {
            return sum / N;
        } else {
            return Double.NaN;
        }*/
    }

    public synchronized double getStandardDeviation() {
        if (dstats != null) {
            return dstats.getStandardDeviation();
        } else {
            return sstats.getStandardDeviation();
        }
        /*if (N > 1) {
            return Math.sqrt(sqsum / (N - 1) - (sum * sum) / (N * (N-1)));
        } else if (N == 1) {
            return 0;
        } else {
            return Double.NaN;
        }*/
    }

    // forces Java to not use scientific notation
    /*private static DecimalFormat doubleFormat = new DecimalFormat("#");
    static {
        doubleFormat.setMaximumFractionDigits(340);
    }*/
    private String format(double d) {
        //return Double.isNaN(d) ? "NaN" : doubleFormat.format(d);
        return Double.toString(d);
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
        if (dstats == null) {
            throw new IllegalStateException();
        }
        try (BufferedWriter br = Files.newBufferedWriter(Paths.get(filename))) {
            br.write("#" + getErrorbars() + "\n");
            br.write(dstats.getMin() + "\t0" + "\n");
            for (int i = 1; i <= 100; ++i) {
                br.write(dstats.getPercentile(i) + "\t" + (i / 100d) + "\n");
            }
        }
    }
}
