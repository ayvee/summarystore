package com.samsung.sra.optimization;

import com.samsung.sra.experiments.Distribution;

import java.util.Random;

public class BinnedStreamGenerator {
    public static long[] generateBinnedStream(int T, Distribution<Long> interarrivals) {
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
