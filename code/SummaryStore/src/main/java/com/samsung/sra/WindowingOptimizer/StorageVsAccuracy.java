package com.samsung.sra.WindowingOptimizer;

import com.samsung.sra.DataStoreExperiments.ExponentialInterarrivals;
import com.samsung.sra.DataStoreExperiments.InterarrivalDistribution;
import com.samsung.sra.DataStoreExperiments.Statistics;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.List;

public class StorageVsAccuracy {
    private static void oneExperiment(int T, double arrivalRate, double queriesZipfS,
                                      double[] storageRatios, int nTrialsPerRatio) throws IOException {
        String filename = String.format("bound-sa_T%d_l%.0f_z%.0f-mean.tsv", T, arrivalRate, (queriesZipfS < 1e-4 ? 0: queriesZipfS));
        System.err.println("=====> " + filename + " <=====");

        InterarrivalDistribution interarrivals = new ExponentialInterarrivals(arrivalRate);
        TMeasure tMeasure = new ZipfTMeasure(T, queriesZipfS);
        Statistics[] results = new Statistics[storageRatios.length];
        for (int sri = 0; sri < storageRatios.length; ++sri) {
            results[sri] = new Statistics(false);
        }
        for (int trial = 0; trial < nTrialsPerRatio; ++trial) {
            long[] counts = BinnedStreamGenerator.generateBinnedStream(T, interarrivals);
            ValueAwareOptimizer optimizer = new ValueAwareOptimizer(T, tMeasure, counts);
            System.err.println("[" + LocalDateTime.now() + "] trial " + trial + ":");
            for (int sri = 0; sri < storageRatios.length; ++sri) {
                int W = (int)Math.ceil(T / storageRatios[sri]);
                List<Integer> bestWindowing = optimizer.optimize(W);
                double bestWindowingError = optimizer.getWAPE(bestWindowing);
                //double bestWindowingError = optimizer.getMeanRelativeError(bestWindowing);
                //double bestWindowingError = optimizer.getMeanMAPE(bestWindowing);
                System.err.println("[" + LocalDateTime.now() + "] error(" + storageRatios[sri] + ") = " + bestWindowingError);
                results[sri].addObservation(bestWindowingError);
            }
        }

        BufferedWriter outWriter = Files.newBufferedWriter(Paths.get(filename));
        for (int sri = 0; sri < storageRatios.length; ++sri) {
            outWriter.write(storageRatios[sri] + "\t" + results[sri].getErrorbars() + "\n");
        }
        outWriter.close();
    }

    public static void main(String[] args) throws IOException {
        int T = 1000;
        double[] arrivalRates = {1000};
        double[] queriesZipfSs = {1e-5, 1, 2};
        double[] storageRatios = {1,
                1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9,
                2, 3, 4, 5, 6, 7, 8, 9,
                10, 20, 30, 40, 50, 60, 70, 80, 90,
                100, 200, 300, 400, 500, 600, 700, 800, 900};
        int nTrialsPerRatio = 1;

        for (double arrivalRate: arrivalRates) {
            for (double queriesZipfS: queriesZipfSs) {
                oneExperiment(T, arrivalRate, queriesZipfS, storageRatios, nTrialsPerRatio);
            }
        }
    }
}
