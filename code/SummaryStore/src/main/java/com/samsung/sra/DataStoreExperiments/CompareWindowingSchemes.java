package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.QueryType;
import com.samsung.sra.DataStore.SummaryStore;
import org.nustaq.serialization.FSTConfiguration;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.function.ToDoubleFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class CompareWindowingSchemes {
    private static final int numAgeLengthClasses = 8;
    private static final int numRandomQueriesPerClass = 1_000;
    private static final ToDoubleFunction<Statistics> metric = stats ->
            //stats.getMean();
            stats.getQuantile(0.95);
    private static final ToDoubleFunction<AgeLengthClass> weightFunction = alClass ->
            1;
            //Math.pow(alClass.lengthClassNum + 1, 0) / Math.pow(alClass.ageClassNum + 1, 2);

    private static Logger logger = LoggerFactory.getLogger(CompareWindowingSchemes.class);
    private static final long streamID = 0;

    private static final FSTConfiguration fstConf;
    static {
        fstConf = FSTConfiguration.createDefaultConfiguration();
        fstConf.registerClass(Statistics.class);
    }

    private static Map<String, SummaryStore> discoverStores(String directory, long N) throws IOException, RocksDBException {
        Pattern pattern = Pattern.compile("(.*N" + N + "\\.D([^\\.]+))\\.bucketStore.*");
        Map<String, SummaryStore> stores = new LinkedHashMap<>();
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(Paths.get(directory), "N" + N + ".*.bucketStore")) {
            for (Path path: paths) {
                if (Files.isDirectory(path)) {
                    Matcher matcher = pattern.matcher(path.toString());
                    if (matcher.matches()) {
                        String prefix = matcher.group(1), decay = matcher.group(2);
                        // WARNING: setting cache size to N, i.e. load everything off RocksDB into memory
                        SummaryStore store = new SummaryStore(prefix, N);
                        stores.put(decay, store);
                        store.warmupCache();
                    }
                }
            }
        }
        return stores;
    }

    /**
     * Compute stats for each (decay function, age length class) pair. Results will be memoized if the argument is true
     */
    private static LinkedHashMap<String, LinkedHashMap<AgeLengthClass, Statistics>> runExperiment(
            String directory, long N, boolean memoize) throws Exception {
        LinkedHashMap<String, LinkedHashMap<AgeLengthClass, Statistics>> results;
        String memoFile = memoize ? String.format("%s/N%d.profile", directory, N) : null;
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
        Map<String, SummaryStore> stores = discoverStores(directory, N);
        if (stores.isEmpty()) {
            throw new IllegalArgumentException("no stores found with N = " + N);
        }
        results = new LinkedHashMap<>();

        //stores.forEach((name, store) -> System.out.println(name + " = " + store.getStoreSizeInBytes() + " bytes"));

        List<AgeLengthClass> alClasses = AgeLengthSampler.getAgeLengthClasses(N, N, numAgeLengthClasses, numAgeLengthClasses);

        stores.keySet().forEach(name -> results.put(name, new LinkedHashMap<>()));
        Random random = new Random();
        alClasses.forEach(alClass -> {
            logger.info("Processing age length class {}", alClass);
            stores.keySet().forEach(name -> results.get(name).put(alClass, new Statistics(true)));
            IntStream.range(0, numRandomQueriesPerClass).parallel().forEach(q -> {
                Pair<Long> ageLength = alClass.sample(random);
                long age = ageLength.first(), length = ageLength.second();
                long l = N - length + 1 - age, r = N - age;
                if (l < 0 || r >= N) return;
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

    public static void main(String[] args) throws Exception {
        String directory;
        long N;
        try {
            if (args.length != 2) throw new IllegalArgumentException("wrong argument count");
            directory = args[0];
            N = Long.parseLong(args[1].replace(",", ""));
        } catch (IllegalArgumentException e) {
            System.out.println("SYNTAX ERROR: " + e.getMessage());
            System.out.println("\tCompareWindowingSchemes <datasets_directory> <N>");
            System.exit(2);
            return;
        }

        LinkedHashMap<String, LinkedHashMap<AgeLengthClass, Statistics>> results = runExperiment(directory, N, true);

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
            decayFunctionCosts.put(decayFunction, cost);
            System.out.println("\t" + decayFunction + " = " + cost);
        });

        String bestDecay = decayFunctionCosts.entrySet().stream().
                min(Comparator.comparing(Map.Entry::getValue)).
                get().getKey();
        System.out.println("Best decay = " + bestDecay);
    }
}
