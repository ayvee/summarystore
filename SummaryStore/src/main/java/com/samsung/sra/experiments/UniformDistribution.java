package com.samsung.sra.experiments;

import com.moandjiezana.toml.Toml;

import java.util.Random;

public class UniformDistribution implements Distribution<Long> {
    private final long min, max;

    public UniformDistribution(Toml conf) {
        this.min = conf.getLong("min");
        this.max = conf.getLong("max");
        assert min <= max;
    }

    @Override
    public Long next(Random random) {
        return min + Math.abs(random.nextLong()) % (max - min + 1);
    }
}
