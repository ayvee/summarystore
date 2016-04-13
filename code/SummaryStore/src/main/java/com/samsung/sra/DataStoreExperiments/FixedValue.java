package com.samsung.sra.DataStoreExperiments;

public class FixedValue implements ValueDistribution {
    private final long value;

    public FixedValue(long value) {
        this.value = value;
    }

    @Override
    public long getNextValue() {
        return value;
    }
}
