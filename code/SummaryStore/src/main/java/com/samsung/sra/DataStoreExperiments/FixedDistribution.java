package com.samsung.sra.DataStoreExperiments;

import java.util.Random;

public class FixedDistribution implements Distribution<Long> {
    private final long interarrival;

    public FixedDistribution(long interarrival) {
        this.interarrival = interarrival;
    }

    @Override
    public Long next(Random random) {
        return interarrival;
    }
}
