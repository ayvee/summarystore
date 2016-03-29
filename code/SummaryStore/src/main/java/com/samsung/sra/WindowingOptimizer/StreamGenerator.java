package com.samsung.sra.WindowingOptimizer;

public class StreamGenerator {
    public static long[] generateBinnedStream(int T, InterarrivalTimes interarrivals) {
        assert T >= 1;
        long[] ret = new long[T];
        double t = 0;
        while (true) {
            t += interarrivals.getNextInterarrival();
            if ((int)t >= T) {
                break;
            } else {
                ret[(int)t] += 1;
            }
        }
        return ret;
    }
}
