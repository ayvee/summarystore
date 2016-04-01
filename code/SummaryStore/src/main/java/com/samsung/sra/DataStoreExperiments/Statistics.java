package com.samsung.sra.DataStoreExperiments;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Statistics {
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
    }

    public synchronized double getAverage() {
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
