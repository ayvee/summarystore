package com.samsung.sra.DataStoreExperiments;

import java.util.Random;
import java.util.function.BiConsumer;

public class IVStreamGenerator implements StreamGenerator {
    private final InterarrivalDistribution interarrivals;
    private final ValueDistribution values;
    private Random random;
    private final long R;

    public IVStreamGenerator(InterarrivalDistribution interarrivals, ValueDistribution values, long randomSeed) {
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
        IVStreamGenerator generator = new IVStreamGenerator(new FixedInterarrival(2), new UniformValues(0, 100), 0);
        BiConsumer<Long, Long> printer = (ts, v) -> System.out.println(ts + "\t" + v);
        System.out.println("=====> reset <====");
        generator.generate(10, printer);
        generator.reset();
        System.out.println("=====> reset <====");
        generator.generate(10, printer);
    }
}
