package com.samsung.sra.DataStoreExperiments;

import java.util.Random;
import java.util.function.BiConsumer;

public class RandomStreamGenerator implements StreamGenerator {
    private final Distribution<Long> interarrivals, values;
    private Random random;
    private final long R;

    public RandomStreamGenerator(Distribution<Long> interarrivals, Distribution<Long> values, long randomSeed) {
        this.interarrivals = interarrivals;
        this.values = values;
        this.R = randomSeed;
        this.random = new Random(R);
    }

    @Override
    public void generate(long T, BiConsumer<Long, Long> consumer) {
        for (long t = 0; t < T; t += interarrivals.next(random)) {
            long v = values.next(random);
            consumer.accept(t, v);
        }
    }

    @Override
    public void reset() {
        random.setSeed(R);
    }

    public static void main(String[] args) {
        RandomStreamGenerator generator = new RandomStreamGenerator(new FixedDistribution(2), new UniformDistribution(0, 100), 0);
        BiConsumer<Long, Long> printer = (ts, v) -> System.out.println(ts + "\t" + v);
        System.out.println("=====> reset <====");
        generator.generate(10, printer);
        generator.reset();
        System.out.println("=====> reset <====");
        generator.generate(10, printer);
    }
}
