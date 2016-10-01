package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.ResultError;
import com.samsung.sra.DataStore.SummaryStore;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

class RunComparison {
    private static Logger logger = LoggerFactory.getLogger(RunComparison.class);
    private static final long streamID = 0;

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

        Workload workload;
        try (InputStream is = Files.newInputStream(Paths.get(workloadFile))) {
            workload = (Workload) SerializationUtils.deserialize(is);
        }
        for (Map.Entry<String, List<Workload.Query>> entry : workload.entrySet()) {
            logger.debug("{}, {}", entry.getKey(), entry.getValue().size());
        }

        HashMap<String, StoreStats> unsorted = new HashMap<>();
        for (String decay: config.getDecayFunctions()) {
            StoreStats storeStats;
            // WARNING: setting cache size to length(all time), i.e. loading all data into main memory
            long cacheSize = config.getTend() - config.getTstart() + 1;
            try (SummaryStore store = new SummaryStore(config.getStorePrefix(decay), cacheSize)) {
                store.warmupCache();

                List<String> queryClasses = new ArrayList<>(workload.keySet());
                storeStats = new StoreStats(store.getStoreSizeInBytes(), queryClasses);
                final Set<String> pending = Collections.synchronizedSet(new HashSet<>(queryClasses));
                queryClasses.parallelStream().forEach(queryClass -> {
                    Statistics stats = storeStats.queryStats.get(queryClass);
                    workload.get(queryClass).parallelStream().forEach(q -> {
                        try {
                            logger.trace("Running query [{}, {}], true answer = {}", q.l, q.r, q.trueAnswer);
                            long trueAnswer = q.trueAnswer;
                            ResultError<Double, Pair<Double, Double>> estimate =
                                    (ResultError<Double, Pair<Double, Double>>) store.query(streamID, q.l, q.r, q.operatorNum, 0.95);
                            //double error = Math.abs(estimate.result - trueAnswer) / (1d + trueAnswer);
                            double error = estimate.error.getFirst() <= trueAnswer && trueAnswer <= estimate.error.getSecond() ? 0 : 1;
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

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("RunComparison", false).
                description("compute statistics for each decay function and query class, " +
                        "and optionally print weighted stats if a weight function and metric are specified").
                defaultHelp(true);
        parser.addArgument("conf").help("config file").type(File.class);
        parser.addArgument("-metric").help("error metric (allowed: \"mean\", \"p<percentile>\", e.g. \"p50\")");
        parser.addArgument("-weight").help("function assigning weights to each query class (allowed: \"uniform\")");
        parser.addArgument("-force-run").help("force running workload, ignoring any memoized results").action(Arguments.storeTrue());

        Configuration config;
        ToDoubleFunction<Statistics> metric;
        ToDoubleFunction<String> weightFunction;
        try {
            Namespace parsed = parser.parseArgs(args);
            config = new Configuration(parsed.get("conf"));
            if (parsed.getBoolean("force_run")) {
                Files.deleteIfExists(Paths.get(config.getProfileFile()));
            }
            String metricName = parsed.get("metric");
            String weightFunctionName = parsed.get("weight");
            if (metricName != null || weightFunctionName != null) { // TODO: move into Configuration?
                if (metricName == null || weightFunctionName == null) {
                    throw new IllegalArgumentException("either both metric and weight function should be specified or neither");
                }
                if (metricName.equalsIgnoreCase("mean")) {
                    metric = Statistics::getMean;
                } else if (metricName.toLowerCase().startsWith("p")) {
                    double quantile = Double.valueOf(metricName.substring(1)) * 0.01;
                    metric = s -> s.getQuantile(quantile);
                } else {
                    throw new IllegalArgumentException("unknown metric " + metricName);
                }
                if (weightFunctionName.equalsIgnoreCase("uniform")) {
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
            results.forEach((decayFunction, stats) -> {
                Collection<Statistics> eachClassStatistics = stats.queryStats.values();
                Collection<Double> eachClassWeight = stats.queryStats.keySet().stream().
                        mapToDouble(weightFunction).
                        boxed().collect(Collectors.toList());
                // construct the aggregate weighted mixture distribution over all the classes
                Statistics mixtureStats = new Statistics(eachClassStatistics, eachClassWeight);
                double cost = metric.applyAsDouble(mixtureStats);
                System.out.println(decayFunction + "\t" + stats.sizeInBytes + "\t" + cost);
            });
        }
    }
}
