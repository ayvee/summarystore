package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.*;
import org.nustaq.serialization.FSTConfiguration;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class CompareWindowingSchemes {
    private static Logger logger = LoggerFactory.getLogger(CompareWindowingSchemes.class);
    //private static String loc_prefix = "/tmp/tdstore_";
    private static final long streamID = 0;

    // BEGIN CONFIG. If these parameters are changed, it may be necessary to delete memoized
    // results (see runExperiment()) to rerun the experiment
    private static long T = 1_000_000;
    private static int storageSavingsFactor = 100;
    // # of windows. Divide by 2 because we maintain 2 aggregates per window, sum and count
    private static int W = (int)(T / storageSavingsFactor / 2);

    // write workload
    private static InterarrivalDistribution interarrivals = new FixedInterarrival(1);
    private static ValueDistribution values = new UniformValues(0, 100);

    // read workload
    private static int numAgeLengthClasses = 8;
    private static int numRandomQueriesPerClass = 10_000;

    // set of decay functions
    private static LinkedHashMap<String, SummaryStore> stores = new LinkedHashMap<>();
    static {
        //Runtime.getRuntime().exec(new String[]{"sh", "-c", "rm -rf " + loc_prefix + "*"}).waitFor();
        try {
            registerStore(stores, "exponential", new CountBasedWBMH(streamID, ExponentialWindowLengths.getWindowingOfSize(T, W)));
            for (int d = 0; d < 10; ++d) {
                registerStore(stores, "polynomial(" + d + ")", new CountBasedWBMH(streamID, PolynomialWindowLengths.getWindowingOfSize(d, T, W)));
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
    // END CONFIG

    private static final FSTConfiguration fstConf;
    static {
        fstConf = FSTConfiguration.createDefaultConfiguration();
        fstConf.registerClass(Statistics.class);
    }

    /**
     * Compute stats for each (decay function, age length class) pair. Results will be memoized if the argument is true
     */
    private static LinkedHashMap<String, LinkedHashMap<AgeLengthClass, Statistics>> runExperiment(boolean memoize) throws Exception {
        LinkedHashMap<String, LinkedHashMap<AgeLengthClass, Statistics>> results;
        String memoFile = memoize ? String.format("compare-windowing.T%d.W%d.dat", T, W) : null;
        if (memoize) {
            try {
                byte[] serialized = Files.readAllBytes(Paths.get(memoFile));
                return (LinkedHashMap<String, LinkedHashMap<AgeLengthClass, Statistics>>)fstConf.asObject(serialized);
            } catch (NoSuchFileException e) {
                logger.info("memoized results not found, running experiment");
            } catch (IOException | ClassCastException e) {
                logger.warn("failed to deserialize memoized results", e);
            }
        }
        results = new LinkedHashMap<>();

        WriteLoadGenerator generator = new WriteLoadGenerator(interarrivals, values, streamID, stores.values());
        generator.generateUntil(T);

        //stores.forEach((name, store) -> System.out.println(name + " = " + store.getStoreSizeInBytes() + " bytes"));

        //Double[] weights = new Double[numAgeLengthClasses * numAgeLengthClasses];
        //Arrays.fill(weights, 1d);
        //AgeLengthSampler querySampler = new AgeLengthSampler(T, T, numAgeLengthClasses, numAgeLengthClasses, Arrays.asList(weights));
        List<AgeLengthClass> alClasses = AgeLengthSampler.getAgeLengthClasses(T, T, numAgeLengthClasses, numAgeLengthClasses);

        stores.keySet().forEach(name -> results.put(name, new LinkedHashMap<>()));
        Random random = new Random();
        alClasses.forEach(alClass -> {
            logger.info("Processing age length class {}", alClass);
            stores.keySet().forEach(name -> results.get(name).put(alClass, new Statistics(true)));
            IntStream.range(0, numRandomQueriesPerClass).parallel().forEach(q -> {
                Pair<Long> ageLength = alClass.sample(random);
                long age = ageLength.first(), length = ageLength.second();
                long l = T - length + 1 - age, r = T - age;
                if (l < 0 || r >= T) return;
                double trueCount = r - l + 1;
                stores.forEach((name, store) -> {
                    try {
                        double estCount = (long)store.query(streamID, l, r, QueryType.COUNT, null);
                        results.get(name).get(alClass).addObservation(estCount / trueCount - 1);
                    } catch (Exception e) { // java streams don't like exceptions, apparently
                        e.printStackTrace();
                    }
                });
            });
        });

        // FIXME: close() stores when done

        if (memoize) {
            try (FileOutputStream fos = new FileOutputStream(memoFile)) {
                fos.write(fstConf.asByteArray(results));
            } catch (IOException e) {
                logger.warn("failed to serialize results to memo file", e);
            }
        }

        return results;
    }

    private static void registerStore(Map<String, SummaryStore> stores, String storeName, WindowingMechanism windowingMechanism) throws RocksDBException, StreamException {
        //SummaryStore store = new SummaryStore(new RocksDBBucketStore(loc_prefix + storeName));
        SummaryStore store = new SummaryStore(null);
        store.registerStream(streamID, windowingMechanism);
        stores.put(storeName, store);
    }

    public static void main(String[] args) throws Exception {
        ToDoubleFunction<Statistics> metric = stats -> stats.getQuantile(0.95);
        ToDoubleFunction<AgeLengthClass> weightFunction = alClass ->
                //1;
                Math.pow(alClass.lengthClassNum + 1, 0) / Math.pow(alClass.ageClassNum + 1, 2);

        LinkedHashMap<String, LinkedHashMap<AgeLengthClass, Statistics>> results = runExperiment(true);

        System.out.println("cost of");
        LinkedHashMap<String, Double> decayFunctionCosts = new LinkedHashMap<>(); // compute cost of each decay function
        results.forEach((decayFunction, perClassResults) -> {
            Collection<Statistics> eachClassStatistics = perClassResults.values();
            Collection<Double> eachClassWeight = perClassResults.keySet().stream().
                    mapToDouble(weightFunction).
                    boxed().collect(Collectors.toList());
            // construct the aggregate weighted mixture distribution over all the classes
            Statistics mixtureStats = new Statistics(eachClassStatistics, eachClassWeight);
            double cost = metric.applyAsDouble(mixtureStats);
            /*// normalize so that sum of all classes' weights = 1
            final double weightNormFact = perClassResults.keySet().stream().mapToDouble(weightFunction).sum();
            // cost of each decay function = weighted sum of value of metric in each class (e.g. weighted mean or weighted median)
            double cost = perClassResults.entrySet().stream().mapToDouble(entry -> {
                AgeLengthClass alClass = entry.getKey();
                Statistics stats = entry.getValue();
                return metric.applyAsDouble(stats) * weightFunction.applyAsDouble(alClass) / weightNormFact;
            }).sum();*/
            decayFunctionCosts.put(decayFunction, cost);
            System.out.println("\t" + decayFunction + " = " + cost);
        });

        String bestDecay = decayFunctionCosts.entrySet().stream().
                min(Comparator.comparing(Map.Entry::getValue)).
                get().getKey();
        System.out.println("Best decay = " + bestDecay);
    }
}
