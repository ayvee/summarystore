package com.samsung.sra.DataStoreExperiments;

public class FixedInterarrival implements InterarrivalDistribution {
    private final long interarrival;

    public FixedInterarrival(long interarrival) {
        this.interarrival = interarrival;
    }

    public long getNextInterarrival() {
        return interarrival;
    }
}
