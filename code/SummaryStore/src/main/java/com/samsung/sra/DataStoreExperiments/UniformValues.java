package com.samsung.sra.DataStoreExperiments;

import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.random.RandomGenerator;

public class UniformValues implements ValueDistribution {
    private final long l, r;
    private final UniformRealDistribution distribution;

    public UniformValues(RandomGenerator rng, long l, long r) {
        this.l = l;
        this.r = r;
        distribution = rng != null ?
                new UniformRealDistribution(rng, l, r) :
                new UniformRealDistribution(l, r);
    }

    public UniformValues(long l, long r) {
        this(null, l, r);
    }

    @Override
    public long getNextValue() {
        long sample = (long)distribution.sample();
        if (sample < l) sample = l;
        if (sample > r) sample = r;
        return sample;
    }
}
