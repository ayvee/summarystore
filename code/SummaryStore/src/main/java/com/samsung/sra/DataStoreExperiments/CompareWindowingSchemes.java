package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.QueryType;
import com.samsung.sra.DataStore.SummaryStore;
import org.nustaq.serialization.FSTConfiguration;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.ToDoubleFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
        fstConf.registerClass(StoreStats.class);
    }

    /** Returns (decay function name) -> (store location prefix) mapping for all stores in the directory
     * with the specified N */
    private static Map<String, String> discoverStores(String directory, long N) throws IOException, RocksDBException {
        Pattern pattern = Pattern.compile("(.*N" + N + "\\.D([^\\.]+))\\.bucketStore.*");
        Map<String, String> stores = new LinkedHashMap<>();
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(Paths.get(directory), "N" + N + ".*.bucketStore")) {
            for (Path path: paths) {
                if (Files.isDirectory(path)) {
                    Matcher matcher = pattern.matcher(path.toString());
                    if (matcher.matches()) {
                        String prefix = matcher.group(1), decay = matcher.group(2);
                        stores.put(decay, prefix);
                    }
                }
            }
        }
        return stores;
    }

    private static class Query {
        final long l, r;
        final QueryType type;
        final Object[] params;

        Query(long l, long r, QueryType type, Object[] params) {
            this.l = l;
            this.r = r;
            this.type = type;
            this.params = params;
        }
    }

    private static class StoreStats implements Serializable {
        final long sizeInBytes;
        final LinkedHashMap<AgeLengthClass, Statistics> queryStats;

        StoreStats(long sizeInBytes, Collection<AgeLengthClass> alClasses) {
            this.sizeInBytes = sizeInBytes;
            queryStats = new LinkedHashMap<>();
            alClasses.forEach(alClass -> queryStats.put(alClass, new Statistics(true)));
        }
    }

    /**
     * Compute stats for each store. Results will be memoized to disk if the argument is true
     */
    private static LinkedHashMap<String, StoreStats> runExperiment(
            String directory, long N, boolean memoize) throws Exception {
        String memoFile = memoize ? String.format("%s/N%d.profile", directory, N) : null;
        if (memoize) {
            try {
                byte[] serialized = Files.readAllBytes(Paths.get(memoFile));
                return (LinkedHashMap<String, StoreStats>)fstConf.asObject(serialized);
            } catch (NoSuchFileException e) {
                logger.info("memoized results not found, running experiment");
            } catch (IOException | ClassCastException e) {
                logger.warn("failed to deserialize memoized results", e);
            }
        }

        Map<String, String> stores = discoverStores(directory, N);
        if (stores.isEmpty()) {
            throw new IllegalArgumentException("no stores found with N = " + N);
        }
        List<AgeLengthClass> alClasses = AgeLengthSampler.getAgeLengthClasses(N, N, numAgeLengthClasses, numAgeLengthClasses);

        Map<AgeLengthClass, List<Query>> workload = new ConcurrentHashMap<>();
        Random random = new Random();
        for (AgeLengthClass alClass: alClasses) {
            List<Query> queries = new ArrayList<>();
            for (int q = 0; q < numRandomQueriesPerClass; ++q) {
                Pair<Long> ageLength = alClass.sample(random);
                long age = ageLength.first(), length = ageLength.second();
                long l = N - length + 1 - age, r = N - age;
                if (0 <= l && r < N) {
                    queries.add(new Query(l, r, QueryType.COUNT, null));
                }
            }
            workload.put(alClass, queries);
        }

        LinkedHashMap<String, StoreStats> results = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry: stores.entrySet()) {
            String decay = entry.getKey();
            StoreStats storeStats;
            // WARNING: setting cache size to N, i.e. loading all data into main memory
            try (SummaryStore store = new SummaryStore(entry.getValue(), N)) {
                store.warmupCache();

                storeStats = new StoreStats(store.getStoreSizeInBytes(), alClasses);
                final Set<AgeLengthClass> pending = Collections.synchronizedSet(new LinkedHashSet<>(alClasses));
                alClasses.parallelStream().forEach(alClass -> {
                    Statistics stats = storeStats.queryStats.get(alClass);
                    workload.get(alClass).parallelStream().forEach(q -> {
                        try {
                            logger.trace("Running query [{}, {}]", q.l, q.r);
                            double trueCount = q.r - q.l + 1;
                            double estCount = (long) store.query(streamID, q.l, q.r, q.type, q.params);
                            stats.addObservation(estCount / trueCount - 1);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                    pending.remove(alClass);
                    synchronized (pending) {
                        if (logger.isInfoEnabled()) {
                            logger.info("pending({}):", decay);
                            for (AgeLengthClass pendingALClass : pending) {
                                System.out.println("\t\t" + pendingALClass);
                            }
                        }
                    }
                });
            }
            results.put(decay, storeStats);
        }

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

        LinkedHashMap<String, StoreStats> results = runExperiment(directory, N, true);

        System.out.println("cost of");
        LinkedHashMap<String, Double> decayFunctionCosts = new LinkedHashMap<>(); // compute cost of each decay function
        results.forEach((decayFunction, stats) -> {
            Collection<Statistics> eachClassStatistics = stats.queryStats.values();
            Collection<Double> eachClassWeight = stats.queryStats.keySet().stream().
                    mapToDouble(weightFunction).
                    boxed().collect(Collectors.toList());
            // construct the aggregate weighted mixture distribution over all the classes
            Statistics mixtureStats = new Statistics(eachClassStatistics, eachClassWeight);
            double cost = metric.applyAsDouble(mixtureStats);
            decayFunctionCosts.put(decayFunction, cost);
            System.out.println("\t" + decayFunction + "(" + (stats.sizeInBytes / 1024.0 / 1024) + " MB) = " + cost);
        });

        String bestDecay = decayFunctionCosts.entrySet().stream().
                min(Comparator.comparing(Map.Entry::getValue)).
                get().getKey();
        System.out.println("Best decay = " + bestDecay);
    }
}
