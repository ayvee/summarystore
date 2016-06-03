package com.samsung.sra.WindowingOptimizer;

import com.samsung.sra.DataStoreExperiments.InterarrivalDistribution;

import java.util.Random;

public class BinnedStreamGenerator {
    public static long[] generateBinnedStream(int T, InterarrivalDistribution interarrivals) {
        assert T >= 1;
        long[] ret = new long[T];
        double t = 0;
        Random random = new Random(0);
        while (true) {
            t += interarrivals.next(random);
            if ((int)t >= T) {
                break;
            } else {
                ret[(int)t] += 1;
            }
        }
        return ret;
    }
}
