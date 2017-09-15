package com.samsung.sra.experiments;

import it.unimi.dsi.util.XoRoShiRo128PlusRandomGenerator;

public class ParRandomStreamIterator {
    //private SplittableRandom random;
    private final XoRoShiRo128PlusRandomGenerator random = new XoRoShiRo128PlusRandomGenerator();
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
        currT += (long) Math.ceil(166.66666667 / Math.pow(random.nextFloat(), 1d / 1.2));
        //currV = random.nextInt(1, 101);
        currV = random.nextInt(100);
    }

    public void reset() {
        //random = new SplittableRandom(R);
        random.setSeed(R);
        currT = T0;
        //currV = random.nextInt(1, 101);
        currV = random.nextInt(100);
    }
}
