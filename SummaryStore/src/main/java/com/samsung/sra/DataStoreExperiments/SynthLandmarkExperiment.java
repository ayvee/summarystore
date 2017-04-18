package com.samsung.sra.DataStoreExperiments;


import com.samsung.sra.DataStore.Aggregates.SumOperator;
import com.samsung.sra.DataStore.*;
import org.apache.commons.lang.StringUtils;

import java.util.Random;
import java.util.TreeSet;

@SuppressWarnings("FieldCanBeLocal")
public class SynthLandmarkExperiment {
    private final long streamID = 0;
    private final Random random = new Random();

    /***** BEGIN CONFIGURABLE VALUES *****/

    private final long T = 52 * 7 * 86400L;
    private final long interarrival = 1;
    //private final Windowing windowing = new RationalPowerWindowing(1, 1, 1, 5); // ~10x compaction on an all-ops sstore
    private final Windowing windowing = new GenericWindowing(new ExponentialWindowLengths(2));

    private final boolean useLandmarks = true;

    /* Change modes every modeChangeInterval, picking a random mode according to modeCDF. Each mode corresponds to a
     * different value distribution. Arrivals are at a uniform 1 per second. */

    private enum Mode {
        LOW,
        MEDIUM,
        HIGH
    }

    /** allowed to be stochastic */
    private long getModeChangeInterval() {
        return 7 * 86400L;
    }

    private final double[] modeCDF = {0.8, 0.9, 1}; // CDF over how often each mode occurs

    private long getRandomValue(Mode mode) {
        switch (mode) {
            case LOW:
                return 1 + random.nextLong() % 80;
            case MEDIUM:
                return 81 + random.nextLong() % 10;
            case HIGH:
                return 91 + random.nextLong() % 10;
            default:
                throw new IllegalStateException("hit unreachable code");
        }
    }

    private final long queryBinSize = 7 * 86400L;

    private final double outlierBinThreshold = 28.87;

    /***** END CONFIGURABLE VALUES *****/

    private static final Mode[] allModes = Mode.values();
    private Mode getRandomMode() {
        double U = random.nextDouble();
        for (int m = 0; m < allModes.length; ++m) {
            if (U <= modeCDF[m]) {
                return allModes[m];
            }
        }
        throw new IllegalStateException("hit unreachable code");
    }

    private final int queryNBins = (int) ((T + queryBinSize - 1) / queryBinSize);

    private int getBin(long t) {
        return (int) (t / queryBinSize);
    }

    private TreeSet<Integer> getOutliers(double[] vals, double threshold) {
        TreeSet<Integer> outliers = new TreeSet<>();
        for (int i = 1; i < vals.length; ++i) {
            if (Math.abs(vals[i] - vals[i-1]) > threshold) {
                outliers.add(i);
            }
        }
        return outliers;
    }

    private void run() throws Exception {
        Mode mode = null;
        long nextModeChange = 0;
        double[] trueSums = new double[queryNBins], estSums = new double[queryNBins];
        try (SummaryStore store = new SummaryStore(null)) {
            store.registerStream(streamID, new CountBasedWBMH(windowing, 1_000_000), new SumOperator());
            for (long t = 0; t < T; t += interarrival) {
                if (t >= nextModeChange) {
                    Mode oldMode = mode;
                    mode = getRandomMode();
                    if (useLandmarks) {
                        if (oldMode == Mode.HIGH && mode != Mode.HIGH) {
                            store.endLandmark(streamID, t - 1);
                        } else if (oldMode != Mode.HIGH && mode == Mode.HIGH) {
                            store.startLandmark(streamID, t);
                        }
                    }
                    nextModeChange += getModeChangeInterval();
                }
                long v = getRandomValue(mode);
                store.append(streamID, t, v);
                trueSums[getBin(t)] += v;
            }
            store.printWindowState(streamID);
            for (int i = 0; i < queryNBins; ++i) {
                estSums[i] = (double) ((ResultError) store.query(streamID, i * queryBinSize, (i+1) * queryBinSize, 0)).result;
            }
        }
        double threshold = outlierBinThreshold * queryBinSize;
        TreeSet<Integer> trueOS = getOutliers(trueSums, threshold), estOS = getOutliers(estSums, threshold);
        System.out.printf("True outliers = %s\n", StringUtils.join(trueOS, ", "));
        System.out.printf("Est. outliers = %s\n", StringUtils.join(estOS, ", "));
    }

    public static void main(String[] args) throws Exception {
        new SynthLandmarkExperiment().run();
    }
}
