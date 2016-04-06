package com.samsung.sra.DataStoreExperiments;

import org.apache.commons.math3.distribution.ExponentialDistribution;

public class ExponentialInterarrivals implements InterarrivalTimes {
    public final double lambda;
    private ExponentialDistribution distr;

    public ExponentialInterarrivals(double lambda) {
        this.lambda = lambda;
        distr = new ExponentialDistribution(1 / lambda);
    }

    public double getNextInterarrival() {
        return distr.sample();
    }
}
