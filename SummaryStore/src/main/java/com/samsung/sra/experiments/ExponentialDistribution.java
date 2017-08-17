package com.samsung.sra.experiments;

import com.moandjiezana.toml.Toml;

import java.util.SplittableRandom;

public class ExponentialDistribution implements Distribution<Long> {
    private final double lambda;

    public ExponentialDistribution(Toml conf) {
        this.lambda = conf.getDouble("lambda");
    }

    @Override
    public Long next(SplittableRandom random) {
        return (long) Math.ceil(-Math.log(random.nextDouble()) / lambda);
    }
}
