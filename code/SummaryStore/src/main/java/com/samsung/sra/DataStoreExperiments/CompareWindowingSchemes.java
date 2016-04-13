package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.*;
import org.rocksdb.RocksDBException;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.IntStream;

class CompareWindowingSchemes {
    private static String loc_prefix = "/tmp/tdstore_";
    private static final StreamID streamID = new StreamID(0);

    public static void main(String[] args) throws Exception {
        long T = 1_000_000;
        int W = 50_000;
        int Q = 1_000_000;

        Runtime.getRuntime().exec(new String[]{"sh", "-c", "rm -rf " + loc_prefix + "*"}).waitFor();
        LinkedHashMap<String, SummaryStore> stores = new LinkedHashMap<>();
        registerStore(stores, "expoential", new CountBasedWBMH(ExponentialWindowLengths.getWindowingOfSize(T, W)));
        //registerStore(stores, "expoential-slow", new SlowCountBasedWBMH(ExponentialWindowLengths.getWindowingOfSize(T, W)));
        registerStore(stores, "linear", new CountBasedWBMH(PolynomialWindowLengths.getWindowingOfSize(1, T, W)));
        registerStore(stores, "constant", new CountBasedWBMH(PolynomialWindowLengths.getWindowingOfSize(0, T, W)));

        InterarrivalDistribution interarrivals = new FixedInterarrival(1);
        ValueDistribution values = new UniformValues(0, 100);
        WriteLoadGenerator generator = new WriteLoadGenerator(interarrivals, values, streamID, stores.values());
        generator.generateUntil(T);

        AgeLengthSampler querySampler = new AgeLengthSampler(T, T,
                3, 6,
                Arrays.asList(
                        1d, 1d, 1d, 1d, 1d, 1d,
                        1d, 1d, 1d, 1d, 1d, 1d,
                        1d, 1d, 1d, 1d, 1d, 1d));

        Map<String, Statistics> inflations = new LinkedHashMap<>();
        for (String store: stores.keySet()) {
            inflations.put(store, new Statistics(false));
        }
        //System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "24");
        IntStream.range(0, Q).parallel().forEach(q -> {
            if (q % 100_000 == 0) {
                System.out.println("[" + LocalDateTime.now() + "] Running query #" + q);
            }
            Pair<Long> ageLength = querySampler.sample();
            long age = ageLength.first(), length = ageLength.second();
            long l = T - length + 1 - age, r = T - age;
            if (l < 0) l = 0;
            if (r >= T) r = T - 1;
            Timestamp lt = new Timestamp(l), rt = new Timestamp(r);
            double trueCount = r - l + 1;
            try {
                for (Map.Entry<String, SummaryStore> entry : stores.entrySet()) {
                    double estCount = (long) entry.getValue().query(streamID, lt, rt, QueryType.COUNT, null);
                    inflations.get(entry.getKey()).addObservation(estCount / trueCount - 1);
                }
            } catch (Exception e) { // java streams don't like exceptions, apparently
                e.printStackTrace();
            }
        });

        for (Map.Entry<String, Statistics> entry: inflations.entrySet()) {
            System.out.println(entry.getKey() + " -> " + entry.getValue().getErrorbars());
        }

        // FIXME: close() stores when done
    }

    private static void registerStore(Map<String, SummaryStore> stores, String name, WindowingMechanism windowingMechanism) throws RocksDBException, StreamException {
        //SummaryStore store = new SummaryStore(new RocksDBBucketStore(loc_prefix + name));
        SummaryStore store = new SummaryStore(new MainMemoryBucketStore());
        store.registerStream(streamID, windowingMechanism);
        stores.put(name, store);
    }
}
