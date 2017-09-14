package com.samsung.sra.experiments;

import java.util.SplittableRandom;

public class ParRandomStreamIterator {
    private SplittableRandom random;
    private final long R;

    public ParRandomStreamIterator(long R) {
        this.R = R;
        reset();
    }

    private long T0, T1;

    public void setTimeRange(long T0, long T1) {
        this.T0 = T0;
        this.T1 = T1;
        reset();
    }

    public long currT, currV;

    public boolean hasNext() {
        return currT <= T1;
    }

    public void next() {
        currT += (long) Math.ceil(166.66666667 / Math.pow(random.nextDouble(), 1d / 1.2));
        currV = random.nextInt(1, 101);
    }

    public void reset() {
        this.random = new SplittableRandom(R);
        this.currT = T0;
        this.currV = random.nextInt(1, 101);
    }
}
