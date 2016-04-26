package com.samsung.sra.DataStoreExperiments;

import org.apache.commons.math3.distribution.UniformRealDistribution;

public class UniformValues implements ValueDistribution {
    private final long l, r;
    private final UniformRealDistribution distribution;

    public UniformValues(long l, long r) {
        this.l = l;
        this.r = r;
        distribution = new UniformRealDistribution(l, r);
    }

    @Override
    public long getNextValue() {
        long sample = (long)distribution.sample();
        if (sample < l) sample = l;
        if (sample > r) sample = r;
        return sample;
    }
}
