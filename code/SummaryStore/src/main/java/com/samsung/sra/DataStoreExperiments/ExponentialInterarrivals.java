package com.samsung.sra.DataStoreExperiments;

import org.apache.commons.math3.distribution.ExponentialDistribution;

public class ExponentialInterarrivals implements InterarrivalDistribution {
    private final double lambda;
    private ExponentialDistribution distr;

    public ExponentialInterarrivals(double lambda) {
        this.lambda = lambda;
        distr = new ExponentialDistribution(1 / lambda);
    }

    public long getNextInterarrival() {
        return Math.max((long)distr.sample(), 1);
    }
}
