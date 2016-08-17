package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.ResultError;
import com.samsung.sra.DataStore.SummaryStore;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.*;
import org.apache.commons.lang.SerializationUtils;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.*;
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
    private static Map<String, String> discoverStores(Configuration config) throws IOException, RocksDBException {
        Pattern pattern = Pattern.compile(String.format("(.*%s\\.D([^\\.]+))\\.bucketStore.*", config.getStorePrefixBasename()));
        Map<String, String> stores = new LinkedHashMap<>();
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(Paths.get(config.getDirectory()), "*.bucketStore")) {
            for (Path path : paths) {
                if (Files.isDirectory(path)) {
                    Matcher matcher = pattern.matcher(path.toString());
                    if (matcher.matches()) {
                        String pathPrefix = matcher.group(1), decay = matcher.group(2);
                        stores.put(decay, pathPrefix);
                    }
                }
            }
        }
        return stores;
    }

    private static class StoreStats implements Serializable {
        final long sizeInBytes;
        final LinkedHashMap<String, Statistics> queryStats;

        StoreStats(long sizeInBytes, Collection<String> queryClasses) {
            this.sizeInBytes = sizeInBytes;
            queryStats = new LinkedHashMap<>();
            queryClasses.forEach(qClass -> queryStats.put(qClass, new Statistics(true)));
        }
    }

    /**
     * Compute stats for each store. Results will be memoized to disk if the argument is true
     */
    private static <R extends Number> Map<String, StoreStats> computeStatistics(
            Configuration config, String workloadFile, String memoFile) throws Exception {
        if (memoFile != null) {
            try (InputStream is = Files.newInputStream(Paths.get(memoFile))){
                return (LinkedHashMap<String, StoreStats>) SerializationUtils.deserialize(is);
            } catch (NoSuchFileException e) {
                logger.info("memoized results not found, running experiment");
            } catch (IOException | ClassCastException e) {
                logger.warn("failed to deserialize memoized results", e);
            }
        }

        Map<String, String> stores = discoverStores(config);
        if (stores.isEmpty()) {
            throw new IllegalArgumentException("no stores found with specified configuration");
        }
        Workload<R> workload = readWorkload(workloadFile);
        if (logger.isDebugEnabled()) {
            for (Map.Entry<String, List<Workload.Query<R>>> entry : workload.entrySet()) {
                logger.debug("{}, {}", entry.getKey(), entry.getValue().size());
            }
        }

        HashMap<String, StoreStats> unsorted = new HashMap<>();
        for (Map.Entry<String, String> entry : stores.entrySet()) {
            String decay = entry.getKey();
            StoreStats storeStats;
            // WARNING: setting cache size to T, i.e. loading all data into main memory
            try (SummaryStore store = new SummaryStore(entry.getValue(), config.getT())) {
                store.warmupCache();

                List<String> queryClasses = new ArrayList<>(workload.keySet());
                storeStats = new StoreStats(store.getStoreSizeInBytes(), queryClasses);
                final Set<String> pending = Collections.synchronizedSet(new HashSet<>(queryClasses));
                queryClasses.parallelStream().forEach(queryClass -> {
                    Statistics stats = storeStats.queryStats.get(queryClass);
                    workload.get(queryClass).parallelStream().forEach(q -> {
                        try {
                            logger.trace("Running query [{}, {}], true answer = {}", q.l, q.r, q.trueAnswer);
                            long trueAnswer = (Long)q.trueAnswer;
                            long estimate = ((ResultError<Long, Long>)store.query(streamID, q.l, q.r, q.operatorNum, q.params)).result;
                            double error = (trueAnswer == estimate) ? 0 :
                                    Math.abs(estimate - trueAnswer) / (1d + trueAnswer);
                            stats.addObservation(error);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                    pending.remove(queryClass);
                    synchronized (pending) {
                        if (logger.isInfoEnabled()) {
                            logger.info("pending({}):", decay);
                            for (String pendingQueryClass : pending) {
                                System.out.println("\t\t" + pendingQueryClass);
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

        if (memoFile != null) {
            try (FileOutputStream fos = new FileOutputStream(memoFile)) {
                SerializationUtils.serialize(sorted, fos);
            } catch (IOException e) {
                logger.warn("failed to serialize results to memo file", e);
            }
        }

        return sorted;
    }

    private static <R> Workload<R> readWorkload(String workloadFile) throws IOException {
        try (InputStream is = Files.newInputStream(Paths.get(workloadFile))) {
            return (Workload<R>) SerializationUtils.deserialize(is);
        }
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("CompareDecayFunctions", false).
                description("compute statistics for each decay function and query class, " +
                        "and optionally print weighted stats if a weight function and metric are specified").
                defaultHelp(true);
        parser.addArgument("conf", "config file").type(File.class);

        parser.addArgument("-metric").help("error metric (allowed: \"mean\", \"p<percentile>\", e.g. \"p50\")");
        parser.addArgument("-weight").help("function assigning weights to each query class (allowed: \"uniform\")");

        Configuration config;
        ToDoubleFunction<Statistics> metric;
        ToDoubleFunction<String> weightFunction;
        try {
            Namespace parsed = parser.parseArgs(args);
            config = new Configuration(parsed.get("conf"));
            String metricName = parsed.get("metric");
            String weightFunctionName = parsed.get("weight");
            if (metricName != null || weightFunctionName != null) { // TODO: move into Configuration?
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

        String workloadFile = config.getWorkloadFile();
        String memoFile = config.getProfileFile();
        Map<String, StoreStats> results = computeStatistics(config, workloadFile, memoFile);

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
