package com.samsung.sra.DataStoreExperiments;

import java.util.Random;

public class ExponentialInterarrivals implements InterarrivalDistribution {
    private final double lambda;

    public ExponentialInterarrivals(double lambda) {
        this.lambda = lambda;
    }

    @Override
    public long next(Random random) {
        return (long) Math.ceil(-Math.log(random.nextDouble()) / lambda);
    }
}
