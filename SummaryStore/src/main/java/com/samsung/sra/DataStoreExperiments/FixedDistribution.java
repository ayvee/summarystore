package com.samsung.sra.DataStoreExperiments;

import com.moandjiezana.toml.Toml;

import java.util.Random;

public class FixedDistribution implements Distribution<Long> {
    private final long value;

    public FixedDistribution(Toml conf) {
        this.value = conf.getLong("value");
    }

    @Override
    public Long next(Random random) {
        return value;
    }
}
