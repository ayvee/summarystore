package com.samsung.sra.experiments;

import com.moandjiezana.toml.Toml;

import java.util.Random;
import java.util.function.Consumer;

public class RandomStreamGenerator implements StreamGenerator {
    private final Distribution<Long> interarrivals, values;
    private Random random;
    private final long R;

    public RandomStreamGenerator(Toml params) {
        this.interarrivals = Configuration.parseDistribution(params.getTable("interarrivals"));
        this.values = Configuration.parseDistribution(params.getTable("values"));
        this.R = params.getLong("random-seed", 0L);
        this.random = new Random(R);
    }

    @Override
    public void generate(long T0, long T1, Consumer<Operation> consumer) {
        for (long t = T0; t <= T1; t += interarrivals.next(random)) {
            Object v = values.next(random);
            consumer.accept(new Operation(Operation.Type.APPEND, t, v));
        }
    }

    @Override
    public void reset() {
        random.setSeed(R);
    }
}
