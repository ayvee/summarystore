package com.samsung.sra.DataStoreExperiments;

import java.util.Random;

public class UniformValues implements ValueDistribution {
    private final long min, max;

    public UniformValues(long min, long max) {
        this.min = min;
        this.max = max;
        assert min <= max;
    }

    @Override
    public long next(Random random) {
        return min + Math.abs(random.nextLong()) % (max - min + 1);
    }
}
