package com.samsung.sra.DataStoreExperiments;

import java.util.Random;

public class UniformValues implements ValueDistribution {
    private final long l, r;

    public UniformValues(long l, long r) {
        this.l = l;
        this.r = r;
        assert l <= r;
    }

    @Override
    public long next(Random random) {
        return l + Math.abs(random.nextLong()) % (r - l + 1);
    }
}
