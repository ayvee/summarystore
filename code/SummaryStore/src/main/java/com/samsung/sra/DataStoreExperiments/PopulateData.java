package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.*;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.RandomGeneratorFactory;

import java.util.Arrays;
import java.util.Random;

// TODO: error recovery
public class PopulateData {
    private static final String outputDirectory = "/mnt/hdd/sstore";
    private static final long streamID = 0;
    
    public static void main(String[] args) throws Exception {
        long N = 1_000_000_000;
        for (double savingsFactor: Arrays.asList(10000, 5000, 2000, 1000, 500, 200, 100, 50, 20, 10, 5, 2)) {
            long W = (long)(N / savingsFactor);
            populateData(N, W, "exponential", ExponentialWindowLengths.getWindowingOfSize(N, W));
            for (int degree: Arrays.asList(0, 1, 2, 4, 8)) {
                populateData(N, W, "polynomial(" + degree + ")", PolynomialWindowLengths.getWindowingOfSize(degree, N, W));
            }
        }
    }

    private static void populateData(long N, long W, String decayName, WindowLengths windowLengths) throws Exception {
        String prefix = String.format("%s/D%s.N%d.W%d", outputDirectory, decayName, N, W);
        SummaryStore store = new SummaryStore(prefix);
        store.registerStream(streamID, new CountBasedWBMH(streamID, windowLengths));
        // NOTE: setting seed to 0 here, guarantees that every decay function will see the same set of values
        RandomGenerator rng = RandomGeneratorFactory.createRandomGenerator(new Random(0));
        InterarrivalDistribution interarrivals = new FixedInterarrival(1);
        ValueDistribution values = new UniformValues(rng, 0, 100);
        WriteLoadGenerator generator = new WriteLoadGenerator(interarrivals, values, streamID, store);
        generator.generateUntil(N);
        store.close();
    }
}
