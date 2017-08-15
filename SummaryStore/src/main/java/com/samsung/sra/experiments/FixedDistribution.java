package com.samsung.sra.experiments;

import com.moandjiezana.toml.Toml;

import java.util.Random;

public class FixedDistribution implements Distribution<Long> {
    private final long value;

    public FixedDistribution(long value) {
        this.value = value;
    }

    public FixedDistribution(Toml conf) {
        this(conf.getLong("value"));
    }

    @Override
    public Long next(Random random) {
        return value;
    }
}
