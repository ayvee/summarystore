package com.samsung.sra.experiments;

import com.samsung.sra.datastore.ResultError;
import com.samsung.sra.datastore.SummaryStore;
import com.samsung.sra.experiments.Workload.Query;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

class RunComparison {
    private static Logger logger = LoggerFactory.getLogger(RunComparison.class);
    private static final long streamID = 0;

    private static class StoreStats implements Serializable {
        final long numValues, numWindows;
        final LinkedHashMap<String, QueryStatistics> queryStats;

        StoreStats(long numValues, long numWindows, Collection<String> queryClasses) {
            this.numValues = numValues;
            this.numWindows = numWindows;
            queryStats = new LinkedHashMap<>();
            queryClasses.forEach(qClass -> queryStats.put(qClass, new QueryStatistics()));
        }
    }

    /**
     * Compute stats for each store. Results will be memoized to disk if the argument is true
     */
    private static Map<String, StoreStats> computeStatistics(
            Configuration config, String workloadFile, String memoFile, Double confidenceLevel) throws Exception {
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
        for (Map.Entry<String, List<Query>> entry : workload.entrySet()) {
            logger.debug("{}, {}", entry.getKey(), entry.getValue().size());
        }

        HashMap<String, StoreStats> unsorted = new HashMap<>();
        for (String decay: config.getDecayFunctions()) {
            StoreStats storeStats;
            // WARNING: setting cache size to length(all time), i.e. loading all data into main memory
            long cacheSize = config.getTend() - config.getTstart() + 1;
            if (config.dropKernelCaches()) {
                try {
                    URL script = RunComparison.class.getClassLoader().getResource("drop-caches.sh");
                    if (script == null) {
                        throw new IllegalStateException("could not find script");
                    }
                    int dropStatus = new ProcessBuilder()
                            .inheritIO() // wire stdout/stderr properly
                            .command("sudo", script.getPath())
                            .start()
                            .waitFor();
                    if (dropStatus != 0) {
                        throw new IllegalStateException("process returned non-zero status " + dropStatus);
                    }
                } catch (IllegalStateException e) {
                    logger.warn("drop-caches failed", e);
                }
            }
            try (SummaryStore store = new SummaryStore(config.getStorePrefix(decay), true, true, cacheSize)) {
                //store.warmupCache();

                List<String> queryClasses = new ArrayList<>(workload.keySet());
                storeStats = new StoreStats(
                        store.getStreamStatistics(streamID).getNumValues(), store.getNumSummaryWindows(streamID), queryClasses);
                final Set<String> pending = Collections.synchronizedSet(new HashSet<>(queryClasses));
                queryClasses.parallelStream().forEach(queryClass -> {
                    QueryStatistics stats = storeStats.queryStats.get(queryClass);
                    workload.get(queryClass).parallelStream().forEach(q -> {
                        try {
                            Object[] params = q.params;
                            if (confidenceLevel != null) {
                                if (params == null || params.length == 0) {
                                    params = new Object[]{confidenceLevel};
                                } else {
                                    Object[] newParams = new Object[params.length + 1];
                                    System.arraycopy(params, 0, newParams, 0, params.length);
                                    newParams[params.length] = confidenceLevel;
                                    params = newParams;
                                }
                            }
                            long ts = System.currentTimeMillis();
                            ResultError re = (ResultError) store.query(streamID, q.l, q.r, q.operatorNum, params);
                            long te = System.currentTimeMillis();
                            logger.trace("Running query [{}, {}] {}, true answer = {}, estimate = {}",
                                    q.l, q.r, q.queryType, q.trueAnswer, re);
                            if (q.queryType != Query.Type.BF && q.queryType != Query.Type.MAX_THRESH) {
                                long trueAnswer = q.trueAnswer.get();
                                stats.addNumericResult(trueAnswer, re, te - ts);
                            } else if (q.queryType == Query.Type.BF) {
                                long longAns = q.trueAnswer.get();
                                assert longAns == 0 || longAns == 1;
                                boolean trueAnswer = (longAns == 1);
                                stats.addBooleanResult(trueAnswer, re, te - ts);
                            } else { // q.queryType == Query.Type.MAX_THRESH
                                long longAns = q.trueAnswer.get();
                                assert longAns == 0 || longAns == 1;
                                boolean trueAnswer = (longAns == 1);
                                ResultError<Boolean, Boolean> reBool = new ResultError<>(
                                        (long) re.result > (long) q.params[0], (Boolean) re.error);
                                stats.addBooleanResult(trueAnswer, reBool, te - ts);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    });
                    pending.remove(queryClass);
                    synchronized (pending) {
                        System.err.println("pending(" + decay + "):");
                        for (String pendingQueryClass : pending) {
                            System.err.println("\t\t" + pendingQueryClass);
                        }
                    }
                });
            }
            unsorted.put(decay, storeStats);
        }
        // sort by store size
        LinkedHashMap<String, StoreStats> sorted = new LinkedHashMap<>();
        unsorted.entrySet().stream().
                sorted(Comparator.comparing(e -> e.getValue().numWindows)).
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
        parser.addArgument("-confidence")
                .help("confidence level")
                .type(Double.class)
                .setDefault(0.95d);
        parser.addArgument("-metrics")
                .help("metrics to print")
                .nargs("+")
                .setDefault("error:p95", "latency:p95", "ci-width:p95", "ci-miss-rate");
        parser.addArgument("-force-run").help("force running workload, ignoring any memoized results").action(Arguments.storeTrue());
        //parser.addArgument("-weight").help("function assigning weights to each query class (allowed: \"uniform\")");

        Configuration config;
        LinkedHashMap<String, ToDoubleFunction<QueryStatistics>> metrics = new LinkedHashMap<>();
        String confidenceLevel;
        //ToDoubleFunction<String> weightFunction;
        try {
            Namespace parsed = parser.parseArgs(args);
            config = new Configuration(parsed.get("conf"));
            confidenceLevel = parsed.getString("confidence");
            if (parsed.getBoolean("force_run")) {
                Files.deleteIfExists(Paths.get(config.getProfileFile(confidenceLevel)));
            }
            for (String metricName : parsed.<String>getList("metrics")) {
                ToDoubleFunction<QueryStatistics> metric;
                if (metricName.equals("ci-miss-rate")) {
                    metric = QueryStatistics::getCIMissRate;
                } else {
                    Function<QueryStatistics, Statistics> statsGetter;
                    String[] vs = metricName.split(":");
                    switch (vs[0]) {
                        case "error":
                            statsGetter = QueryStatistics::getErrorStats;
                            break;
                        case "latency":
                            statsGetter = QueryStatistics::getLatencyStats;
                            break;
                        case "ci-width":
                            statsGetter = QueryStatistics::getCIWidthStats;
                            break;
                        default:
                            throw new IllegalArgumentException("unknown metric " + metricName);
                    }
                    if (vs[1].equals("mean")) {
                        metric = qs -> statsGetter.apply(qs).getMean();
                    } else if (vs[1].startsWith("p")) {
                        double quantile = Double.valueOf(vs[1].substring(1)) * 0.01;
                        metric = qs -> statsGetter.apply(qs).getQuantile(quantile);
                    } else {
                        throw new IllegalArgumentException("unknown metric " + vs[1]);
                    }
                }
                metrics.put(metricName, metric);
            }
            /*String weightFunctionName = parsed.get("weight");
            if (metricName != null || weightFunctionName != null) {
                if (metricName == null || weightFunctionName == null) {
                    throw new IllegalArgumentException("either both metric and weight function should be specified or neither");
                }
                if (weightFunctionName.equalsIgnoreCase("uniform")) {
                    weightFunction = e -> 1;
                } else {
                    throw new IllegalArgumentException("unknown weight function " + weightFunctionName);
                }
            } else {
                metric = null;
                weightFunction = null;
            }*/
        } catch (ArgumentParserException | IllegalArgumentException e) {
            System.err.println("ERROR: " + e.getMessage());
            parser.printHelp(new PrintWriter(System.err, true));
            System.exit(2);
            return;
        }

        String workloadFile = config.getWorkloadFile();
        String memoFile = config.getProfileFile(confidenceLevel);
        Double confidence = confidenceLevel != null ? Double.parseDouble(confidenceLevel) : null;
        Map<String, StoreStats> results = computeStatistics(config, workloadFile, memoFile, confidence);

        if (!metrics.isEmpty()) {
            System.out.print("#decay\t# windows in store\t# elements in stream\tquery\tage class\tlength class");
            for (String metricName: metrics.keySet()) {
                System.out.print("\t" + metricName);
            }
            System.out.println();
            for (Map.Entry<String, StoreStats> statsEntry: results.entrySet()) {
                String decay = statsEntry.getKey();
                StoreStats storeStats = statsEntry.getValue();
                for (Map.Entry<String, QueryStatistics> groupEntry: storeStats.queryStats.entrySet()) {
                    String group = groupEntry.getKey();
                    QueryStatistics stats = groupEntry.getValue();
                    System.out.printf("%s\t%d\t%d\t%s", decay, storeStats.numWindows, storeStats.numValues, group);
                    for (ToDoubleFunction<QueryStatistics> statsFunc: metrics.values()) {
                        System.out.printf("\t%f", statsFunc.applyAsDouble(stats));
                    }
                    System.out.println();
                }
            }
        }

        /*{
            System.out.println("#decay\tstore size (#windows)\tquery\tage class\tlength class\terror:p95\tlatency:p95\tci-width:p95");
            List<Function<QueryStatistics, Statistics>> statsGetters = Arrays.asList(
                    QueryStatistics::getErrorStats,
                    QueryStatistics::getLatencyStats,
                    QueryStatistics::getCIWidthStats
            );
            List<Double> cdfWeights = Arrays.asList(1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d); // 8 for Q0-Q7
            for (Map.Entry<String, StoreStats> statsEntry: results.entrySet()) { // iterate over decay functions
                String decay = statsEntry.getKey();
                StoreStats storeStats = statsEntry.getValue();
                Map<String, List<QueryStatistics>> allForALClass = new LinkedHashMap<>(); // group by AL Class
                for (Map.Entry<String, QueryStatistics> groupEntry: storeStats.queryStats.entrySet()) {
                    String group = groupEntry.getKey();
                    QueryStatistics stats = groupEntry.getValue();
                    String alClass = group.split("\t", 2)[1];
                    if (!allForALClass.containsKey(alClass)) {
                        allForALClass.put(alClass, new ArrayList<>());
                    }
                    allForALClass.get(alClass).add(stats);
                }
                for (Map.Entry<String, List<QueryStatistics>> alEntry: allForALClass.entrySet()) {
                    String alClass = alEntry.getKey();
                    assert alEntry.getValue().size() == cdfWeights.size();
                    List<Statistics> errorCDFs = new ArrayList<>(), latencyCDFs = new ArrayList<>(), ciWidthCDFs = new ArrayList<>();
                    for (QueryStatistics qs: alEntry.getValue()) {
                        errorCDFs.add(qs.getErrorStats());
                        latencyCDFs.add(qs.getLatencyStats());
                        ciWidthCDFs.add(qs.getCIWidthStats());
                    }
                    Statistics
                            errorCDF = new Statistics(errorCDFs, cdfWeights),
                            latencyCDF = new Statistics(latencyCDFs, cdfWeights),
                            ciWidthCDF = new Statistics(ciWidthCDFs, cdfWeights);
                    System.out.printf("%s\t%d\t%d\tCMS\t%s\t%f\t%f\t%f\n", decay, storeStats.numWindows, storeStats.numValues, alClass,
                            errorCDF.getQuantile(0.95), latencyCDF.getQuantile(0.95), ciWidthCDF.getQuantile(0.95));
                }
            }
        }*/
    }
}
