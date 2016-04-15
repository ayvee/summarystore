package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.*;
import org.rocksdb.RocksDBException;

import java.util.*;
import java.util.stream.IntStream;

class CompareWindowingSchemes {
    private static String loc_prefix = "/tmp/tdstore_";
    private static final StreamID streamID = new StreamID(0);

    public static void main(String[] args) throws Exception {
        long T = 1_000_000;
        int W = 50_000;
        int QperClass = 10_000;

        Runtime.getRuntime().exec(new String[]{"sh", "-c", "rm -rf " + loc_prefix + "*"}).waitFor();
        LinkedHashMap<String, SummaryStore> stores = new LinkedHashMap<>();
        registerStore(stores, "exponential", new CountBasedWBMH(ExponentialWindowLengths.getWindowingOfSize(T, W)));
        registerStore(stores, "linear", new CountBasedWBMH(PolynomialWindowLengths.getWindowingOfSize(1, T, W)));
        registerStore(stores, "constant", new CountBasedWBMH(PolynomialWindowLengths.getWindowingOfSize(0, T, W)));

        InterarrivalDistribution interarrivals = new FixedInterarrival(1);
        ValueDistribution values = new UniformValues(0, 100);
        WriteLoadGenerator generator = new WriteLoadGenerator(interarrivals, values, streamID, stores.values());
        generator.generateUntil(T);

        int C = 8;
        Double[] weights = new Double[C * C];
        Arrays.fill(weights, 1d);
        AgeLengthSampler querySampler = new AgeLengthSampler(T, T, C, C, Arrays.asList(weights));

        LinkedHashMap<String, Integer> winCounts = new LinkedHashMap<>();
        stores.keySet().forEach(name -> winCounts.put(name, 0));

        System.out.println("#class\tdecay function\tmean\t50th\t95th\t99th");
        Random random = new Random();
        querySampler.getAllClasses().forEach(alClass -> {
            Map<String, Statistics> inflations = new LinkedHashMap<>();
            stores.keySet().forEach(name -> inflations.put(name, new Statistics(true)));
            IntStream.range(0, QperClass).parallel().forEach(q -> {
                Pair<Long> ageLength = alClass.sample(random);
                long age = ageLength.first(), length = ageLength.second();
                long l = T - length + 1 - age, r = T - age;
                if (l < 0 || r >= T) return;
                Timestamp lt = new Timestamp(l), rt = new Timestamp(r);
                double trueCount = r - l + 1;
                stores.forEach((name, store) -> {
                    try {
                        double estCount = (long)store.query(streamID, lt, rt, QueryType.COUNT, null);
                        inflations.get(name).addObservation(estCount / trueCount - 1);
                    } catch (Exception e) { // java streams don't like exceptions, apparently
                        e.printStackTrace();
                    }
                });
            });
            inflations.forEach((storeName, stats) ->
                System.out.println(alClass + "\t" + storeName + "\t" +
                        stats.getErrorbars()));
                        /*format(stats.getMean()) + "\t" +
                        format(stats.getICDF(0.5)) + "\t" +
                        format(stats.getICDF(0.95)) + "\t" +
                        format(stats.getICDF(0.99)));*/
            String winner = inflations.entrySet().stream().
                    min(Comparator.comparing(e -> e.getValue().getMean())).get().getKey();
            winCounts.put(winner, winCounts.get(winner) + 1);
        });

        System.out.println("winCounts");
        winCounts.forEach((name, count) -> System.out.println(name + "\t" + count));

        // FIXME: close() stores when done
    }

    private static String format(double d) {
        return String.format("%f", d);
    }

    private static void registerStore(Map<String, SummaryStore> stores, String name, WindowingMechanism windowingMechanism) throws RocksDBException, StreamException {
        //SummaryStore store = new SummaryStore(new RocksDBBucketStore(loc_prefix + name));
        SummaryStore store = new SummaryStore(new MainMemoryBucketStore());
        store.registerStream(streamID, windowingMechanism);
        stores.put(name, store);
    }
}
