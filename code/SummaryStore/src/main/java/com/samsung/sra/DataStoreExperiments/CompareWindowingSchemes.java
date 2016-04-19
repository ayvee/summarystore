package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.*;
import org.nustaq.serialization.FSTConfiguration;
import org.rocksdb.RocksDBException;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.ToDoubleFunction;
import java.util.stream.IntStream;

class CompareWindowingSchemes {
    private static String loc_prefix = "/tmp/tdstore_";
    private static final StreamID streamID = new StreamID(0);
    private static final FSTConfiguration fstConf;

    static {
        fstConf = FSTConfiguration.createDefaultConfiguration();
        fstConf.registerClass(Statistics.class);
    }

    /**
     * Compute stats for each (decay function, age length class) pair. Results will be memoized to
     * memoFile if it is not a null argument
     */
    static LinkedHashMap<String, LinkedHashMap<AgeLengthClass, Statistics>> runExperiment(String memoFile) throws Exception {
        long T = 1_000_000;
        int W = (int)(T / 100 / 2);
        int QperClass = 10_000;

        LinkedHashMap<String, LinkedHashMap<AgeLengthClass, Statistics>> results;
        if (memoFile != null) {
            try {
                byte[] serialized = Files.readAllBytes(Paths.get(memoFile));
                return (LinkedHashMap<String, LinkedHashMap<AgeLengthClass, Statistics>>)fstConf.asObject(serialized);
            } catch (IOException | ClassCastException e) {
                System.out.println("WARNING: " + e);
            }
        }
        results = new LinkedHashMap<>();

        Runtime.getRuntime().exec(new String[]{"sh", "-c", "rm -rf " + loc_prefix + "*"}).waitFor();
        LinkedHashMap<String, SummaryStore> stores = new LinkedHashMap<>();
        registerStore(stores, "exponential", new CountBasedWBMH(ExponentialWindowLengths.getWindowingOfSize(T, W)));
        for (int d = 0; d < 10; ++d) {
            registerStore(stores, "polynomial(" + d + ")", new CountBasedWBMH(PolynomialWindowLengths.getWindowingOfSize(d, T, W)));
        }

        InterarrivalDistribution interarrivals = new FixedInterarrival(1);
        ValueDistribution values = new UniformValues(0, 100);
        WriteLoadGenerator generator = new WriteLoadGenerator(interarrivals, values, streamID, stores.values());
        generator.generateUntil(T);

        stores.forEach((name, store) -> {
            System.out.println(name + " = " + store.getStoreSizeInBytes() + " bytes");
            /*try {
                store.printBucketState(streamID);
            } catch (Exception e) {
                e.printStackTrace();
            }*/
        });

        int C = 8;
        Double[] weights = new Double[C * C];
        Arrays.fill(weights, 1d);
        AgeLengthSampler querySampler = new AgeLengthSampler(T, T, C, C, Arrays.asList(weights));

        stores.keySet().forEach(name -> results.put(name, new LinkedHashMap<>()));
        Random random = new Random();
        querySampler.getAllClasses().forEach(alClass -> {
            System.out.println("Processing age length class " + alClass);
            stores.keySet().forEach(name -> results.get(name).put(alClass, new Statistics(true)));
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
                        results.get(name).get(alClass).addObservation(estCount / trueCount - 1);
                    } catch (Exception e) { // java streams don't like exceptions, apparently
                        e.printStackTrace();
                    }
                });
            });
        });

        // FIXME: close() stores when done

        if (memoFile != null) {
            try (FileOutputStream fos = new FileOutputStream(memoFile)) {
                fos.write(fstConf.asByteArray(results));
            } catch (IOException e) {
                System.out.println("WARNING: " + e);
            }
        }

        return results;
    }

    private static void registerStore(Map<String, SummaryStore> stores, String name, WindowingMechanism windowingMechanism) throws RocksDBException, StreamException {
        //SummaryStore store = new SummaryStore(new RocksDBBucketStore(loc_prefix + name));
        SummaryStore store = new SummaryStore(new MainMemoryBucketStore());
        store.registerStream(streamID, windowingMechanism);
        stores.put(name, store);
    }

    public static void main(String[] args) throws Exception {
        ToDoubleFunction<Statistics> metric = stats -> stats.getMean();
        ToDoubleFunction<AgeLengthClass> weightFunction = alClass -> 1d / Math.pow(alClass.ageClassNum + alClass.lengthClassNum + 1, 0.01);

        LinkedHashMap<String, LinkedHashMap<AgeLengthClass, Statistics>> results = runExperiment("compare-windowing.results");
        LinkedHashMap<String, Double> decayFunctionCosts = new LinkedHashMap<>(); // compute cost of each decay function
        results.forEach((decayFunction, perClassResults) -> {
            // cost of each decay function = weighted sum of value of metric in each class
            double cost = perClassResults.entrySet().stream().mapToDouble(entry -> {
                AgeLengthClass alClass = entry.getKey();
                Statistics stats = entry.getValue();
                return metric.applyAsDouble(stats) * weightFunction.applyAsDouble(alClass);
            }).sum();
            decayFunctionCosts.put(decayFunction, cost);
            System.out.println("Cost of " + decayFunction + " = " + cost);
        });
        String bestDecay = decayFunctionCosts.entrySet().stream().min(Comparator.comparing(Map.Entry::getValue)).get().getKey();
        System.out.println("Best decay = " + bestDecay);
    }
}
