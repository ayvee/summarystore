package com.samsung.sra.experiments;

import java.util.SplittableRandom;

public class RandomStreamIterator {
    private final Distribution<Long> interarrivals, values;
    private SplittableRandom random;
    private final long R;

    public RandomStreamIterator(Distribution<Long> interarrivals, Distribution<Long> values, long R) {
        this.interarrivals = interarrivals;
        this.values = values;
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
        currT += interarrivals.next(random);
        currV = values.next(random);
    }

    public void reset() {
        this.random = new SplittableRandom(R);
        this.currT = T0;
        this.currV = values.next(random);
    }
}
