package com.samsung.sra.DataStoreExperiments;

import com.moandjiezana.toml.Toml;

import java.util.Random;

public class ExponentialDistribution implements Distribution<Long> {
    private final double lambda;

    public ExponentialDistribution(Toml conf) {
        this.lambda = conf.getDouble("lambda");
    }

    @Override
    public Long next(Random random) {
        return (long) Math.ceil(-Math.log(random.nextDouble()) / lambda);
    }
}
