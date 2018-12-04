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
package com.samsung.sra.optimization;

import com.moandjiezana.toml.Toml;
import com.samsung.sra.experiments.Distribution;
import com.samsung.sra.experiments.ExponentialDistribution;
import com.samsung.sra.experiments.Statistics;

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

        Distribution<Long> interarrivals = new ExponentialDistribution(new Toml().read("lambda = " + arrivalRate));
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
