package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.QueryType;
import com.samsung.sra.DataStore.SummaryStore;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.*;
import org.apache.commons.lang.SerializationUtils;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.ToDoubleFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class CompareDecayFunctions {
    private static Logger logger = LoggerFactory.getLogger(CompareDecayFunctions.class);
    private static final long streamID = 0;

    /**
     * Returns (decay function name) -> (store location prefix) mapping for all stores in the directory
     * with the specified N
     */
    private static Map<String, String> discoverStores(String directory, long T, String I, String V, long R) throws IOException, RocksDBException {
        Pattern pattern = Pattern.compile(String.format("(.*T%d\\.I%s\\.V%s\\.R%d\\.D([^\\.]+))\\.bucketStore.*", T, I, V, R));
        Map<String, String> stores = new LinkedHashMap<>();
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(Paths.get(directory), "T*.bucketStore")) {
            for (Path path : paths) {
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
    private static LinkedHashMap<String, StoreStats> computeStatistics(
            String directory, long T, String I, String V, long R, int A, int L, int Q,
            boolean memoize) throws Exception {
        String memoFile = memoize ? String.format("%s/T%d.I%s.V%s.R%d.A%d.L%d.Q%d.profile", directory,
                T, I, V, R, A, L, Q) : null;
        if (memoize) {
            try {
                return (LinkedHashMap<String, StoreStats>) SerializationUtils.deserialize(Files.newInputStream(Paths.get(memoFile)));
            } catch (NoSuchFileException e) {
                logger.info("memoized results not found, running experiment");
            } catch (IOException | ClassCastException e) {
                logger.warn("failed to deserialize memoized results", e);
            }
        }

        Map<String, String> stores = discoverStores(directory, T, I, V, R);
        if (stores.isEmpty()) {
            throw new IllegalArgumentException(String.format("no stores found with T, I, V, R = %d, %s, %s, %d", T, I, V, R));
        }
        List<AgeLengthClass> alClasses = AgeLengthSampler.getAgeLengthClasses(T, T, A, L);

        Map<AgeLengthClass, List<Query>> workload = new ConcurrentHashMap<>();
        Random random = new Random();
        for (AgeLengthClass alClass : alClasses) {
            List<Query> queries = new ArrayList<>();
            for (int q = 0; q < Q; ++q) {
                Pair<Long> ageLength = alClass.sample(random);
                long age = ageLength.first(), length = ageLength.second();
                long l = T - length + 1 - age, r = T - age;
                if (0 <= l && r < T) {
                    queries.add(new Query(l, r, QueryType.COUNT, null));
                }
            }
            workload.put(alClass, queries);
        }

        HashMap<String, StoreStats> unsorted = new HashMap<>();
        for (Map.Entry<String, String> entry : stores.entrySet()) {
            String decay = entry.getKey();
            StoreStats storeStats;
            // WARNING: setting cache size to T, i.e. loading all data into main memory
            try (SummaryStore store = new SummaryStore(entry.getValue(), T)) {
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
            unsorted.put(decay, storeStats);
        }
        // sort by store size
        LinkedHashMap<String, StoreStats> sorted = new LinkedHashMap<>();
        unsorted.entrySet().stream().
                sorted(Comparator.comparing(e -> e.getValue().sizeInBytes)).
                forEach(e -> sorted.put(e.getKey(), e.getValue()));

        if (memoize) {
            try (FileOutputStream fos = new FileOutputStream(memoFile)) {
                SerializationUtils.serialize(sorted, fos);
            } catch (IOException e) {
                logger.warn("failed to serialize results to memo file", e);
            }
        }

        return sorted;
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("CompareDecayFunctions", false).
                description("compute statistics for each decay function and age length class, " +
                        "and optionally print weighted stats if a weight function and metric are specified").
                defaultHelp(true);
        ArgumentType<Long> CommaSeparatedLong = (ArgumentParser argParser, Argument arg, String value) ->
                Long.valueOf(value.replace(",", ""));
        parser.addArgument("directory").help("directory containing input SummaryStores; also where output profile will be written");
        parser.addArgument("T").help("size of stream").type(CommaSeparatedLong);
        parser.addArgument("-I")
                .help("interarrival distribution [" + CLIParser.getValidInterarrivalDistributions() + "]")
                .setDefault("fixed1");
        parser.addArgument("-V")
                .help("value distribution [" + CLIParser.getValidValueDistributions() + "]")
                .setDefault("uniform0,100");
        parser.addArgument("-R").help("stream generator RNG seed").type(Long.class).setDefault(0L);
        parser.addArgument("-A").help("number of age classes").type(int.class).setDefault(8);
        parser.addArgument("-L").help("number of length classes").type(int.class).setDefault(8);
        parser.addArgument("-Q").help("number of random queries to run per class").type(int.class).setDefault(1000);
        parser.addArgument("-metric").help("error metric (allowed: \"mean\", \"p<percentile>\", e.g. \"p50\")");
        parser.addArgument("-weight").help("weight function (allowed: \"uniform\")");

        String directory;
        long T;
        String I, V;
        long R;
        ToDoubleFunction<Statistics> metric;
        ToDoubleFunction<AgeLengthClass> weightFunction;
        int A, L, Q;
        try {
            Namespace parsed = parser.parseArgs(args);
            directory = parsed.get("directory");
            T = parsed.get("T");
            I = parsed.get("I");
            V = parsed.get("V");
            R = parsed.get("R");
            // we're silently discarding the output of the parse func calls (only doing them to sanity check CLI args)
            CLIParser.parseInterarrivalDistribution(I);
            CLIParser.parseValueDistribution(V);
            A = parsed.get("A");
            L = parsed.get("L");
            Q = parsed.get("Q");
            String metricName = parsed.get("metric");
            String weightFunctionName = parsed.get("weight");
            if (metricName != null || weightFunctionName != null) { // TODO? move into CLIParser
                if (metricName == null || weightFunctionName == null) {
                    throw new IllegalArgumentException("either both metric and weight function should be specified or neither");
                }
                if (metricName.equals("mean")) {
                    metric = Statistics::getMean;
                } else if (metricName.startsWith("p")) {
                    double quantile = Double.valueOf(metricName.substring(1)) * 0.01;
                    metric = s -> s.getQuantile(quantile);
                } else {
                    throw new IllegalArgumentException("unknown metric " + metricName);
                }
                if (weightFunctionName.equals("uniform")) {
                    weightFunction = e -> 1;
                } else {
                    throw new IllegalArgumentException("unknown weight function " + weightFunctionName);
                }
            } else {
                metric = null;
                weightFunction = null;
            }
        } catch (ArgumentParserException | IllegalArgumentException e) {
            System.err.println("ERROR: " + e.getMessage());
            parser.printHelp(new PrintWriter(System.err, true));
            System.exit(2);
            return;
        }

        LinkedHashMap<String, StoreStats> results = computeStatistics(directory, T, I, V, R, A, L, Q, true);

        if (metric != null) {
            System.out.println("#decay\tstore size (bytes)\tcost");
            //LinkedHashMap<String, Double> decayFunctionCosts = new LinkedHashMap<>(); // compute cost of each decay function
            results.forEach((decayFunction, stats) -> {
                Collection<Statistics> eachClassStatistics = stats.queryStats.values();
                Collection<Double> eachClassWeight = stats.queryStats.keySet().stream().
                        mapToDouble(weightFunction).
                        boxed().collect(Collectors.toList());
                // construct the aggregate weighted mixture distribution over all the classes
                Statistics mixtureStats = new Statistics(eachClassStatistics, eachClassWeight);
                double cost = metric.applyAsDouble(mixtureStats);
                //decayFunctionCosts.put(decayFunction, cost);
                System.out.println(decayFunction + "\t" + stats.sizeInBytes + "\t" + cost);
            });
        }

        /*String bestDecay = decayFunctionCosts.entrySet().stream().
                min(Comparator.comparing(Map.Entry::getValue)).
                get().getKey();
        System.out.println("Best decay = " + bestDecay);*/
    }
}
