package com.samsung.sra.DataStoreExperiments;

import java.util.Random;

public class FixedInterarrival implements InterarrivalDistribution {
    private final long interarrival;

    public FixedInterarrival(long interarrival) {
        this.interarrival = interarrival;
    }

    @Override
    public long next(Random random) {
        return interarrival;
    }
}
