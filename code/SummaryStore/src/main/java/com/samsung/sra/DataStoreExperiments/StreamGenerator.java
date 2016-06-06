package com.samsung.sra.DataStoreExperiments;

import java.util.Random;
import java.util.function.BiConsumer;

/**
 * TODO: turn into an interface, to allow for dependent arrival time and value processes
 */
public class StreamGenerator {
    private final InterarrivalDistribution interarrivals;
    private final ValueDistribution values;
    private Random random;

    public StreamGenerator(InterarrivalDistribution interarrivals, ValueDistribution values, long randomSeed) {
        this.interarrivals = interarrivals;
        this.values = values;
        this.random = new Random(randomSeed);
    }

    public void generate(long T, BiConsumer<Long, Long> consumer) {
        for (long t = 0; t < T; t += interarrivals.next(random)) {
            long v = values.next(random);
            consumer.accept(t, v);
        }
    }

    public void reset(long randomSeed) {
        random.setSeed(randomSeed);
    }

    public static void main(String[] args) {
        StreamGenerator generator = new StreamGenerator(new FixedInterarrival(2), new UniformValues(0, 100), 0);
        BiConsumer<Long, Long> printer = (ts, v) -> System.out.println(ts + "\t" + v);
        System.out.println("=====> reset <====");
        generator.generate(10, printer);
        generator.reset(0);
        System.out.println("=====> reset <====");
        generator.generate(10, printer);
    }
}
