package com.samsung.sra.DataStoreExperiments;

import java.util.Random;

public class ExponentialDistribution implements Distribution<Long> {
    private final double lambda;

    public ExponentialDistribution(double lambda) {
        this.lambda = lambda;
    }

    @Override
    public Long next(Random random) {
        return (long) Math.ceil(-Math.log(random.nextDouble()) / lambda);
    }
}
