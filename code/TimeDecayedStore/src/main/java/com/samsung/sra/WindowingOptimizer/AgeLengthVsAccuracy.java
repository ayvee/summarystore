package com.samsung.sra.WindowingOptimizer;

import com.samsung.sra.DataStoreExperiments.Statistics;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.List;

public class AgeLengthVsAccuracy {
    private static void oneExperiment(int T, double arrivalRate, double queriesZipfS, double storageRatio) throws IOException {
        String outprefix = String.format("bound-al_T%d_l%.0f_z%.0f_s%.0f",
                T, arrivalRate, (queriesZipfS < 1e-4 ? 0: queriesZipfS), storageRatio);
        System.err.println("[" + LocalDateTime.now() + "] " + outprefix);

        InterarrivalTimes interarrivals = new ExponentialInterarrivals(arrivalRate);
        TMeasure tMeasure = new ZipfTMeasure(T, queriesZipfS);
        long[] counts = StreamGenerator.generateBinnedStream(T, interarrivals);
        for (int i = 0; i < counts.length; ++i) {
            counts[i] = 1;
        }
        ValueAwareOptimizer optimizer = new ValueAwareOptimizer(T, tMeasure, counts);
        int W = (int)Math.ceil(T / storageRatio);
        List<Integer> optimalWindowing = optimizer.optimize(W);
        //optimizer.print_E();

        Statistics cdf = new Statistics(true);
        BufferedWriter outWriter = Files.newBufferedWriter(Paths.get(outprefix + ".tsv"));
        outWriter.write("#windowing =");
        for (Integer length: optimalWindowing) {
            outWriter.write(" " + length);
        }
        outWriter.write("; expected error = " + optimizer.getCost(optimalWindowing) + "\n");
        for (int a = 0; a < T; ++a) {
            for (int l = 1; a + l - 1 < T; ++l) {
                double err = optimizer.getQueryCostAL(optimalWindowing, a, l);
                cdf.addObservation(err);
                outWriter.write((a+1) + "\t" + l + "\t" + err + "\n");
            }
        }
        outWriter.close();
        cdf.writeCDF(outprefix + ".cdf");
    }

    public static void main(String[] args) throws IOException {
        int T = 1000;
        double arrivalRate = 1000;
        double[] queriesZipfSs = {1e-5, 1, 2};
        double[] storageRatios = {10, 100};

        for (double queriesZipfS: queriesZipfSs) {
            for (double storageRatio: storageRatios) {
                oneExperiment(T, arrivalRate, queriesZipfS, storageRatio);
            }
        }
    }
}
