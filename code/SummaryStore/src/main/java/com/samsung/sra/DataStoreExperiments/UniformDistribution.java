package com.samsung.sra.DataStoreExperiments;

import java.util.Random;

public class UniformDistribution implements Distribution<Long> {
    private final long min, max;

    public UniformDistribution(long min, long max) {
        this.min = min;
        this.max = max;
        assert min <= max;
    }

    @Override
    public Long next(Random random) {
        return min + Math.abs(random.nextLong()) % (max - min + 1);
    }
}
