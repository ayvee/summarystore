package com.samsung.sra.DataStoreExperiments;

import com.moandjiezana.toml.Toml;

import java.util.Random;

public class ParetoDistribution implements Distribution<Long> {
    private final double xm, alpha;

    public ParetoDistribution(Toml conf) {
        this.xm = conf.getDouble("xm");
        this.alpha = conf.getDouble("alpha");
    }

    @Override
    public Long next(Random random) {
        return (long)Math.ceil(xm / Math.pow(random.nextDouble(), 1 / alpha));
    }
}
