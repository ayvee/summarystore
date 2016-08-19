package com.samsung.sra.DataStoreExperiments;

import java.util.Random;

public class ParetoDistribution implements Distribution<Long> {
    private final double xm, alpha;

    public ParetoDistribution(double xm, double alpha) {
        this.xm = xm;
        this.alpha = alpha;
    }

    @Override
    public Long next(Random random) {
        return (long)Math.ceil(xm / Math.pow(random.nextDouble(), 1 / alpha));
    }
}
