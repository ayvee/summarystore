package com.samsung.sra.experiments;

import com.samsung.sra.datastore.SummaryStore;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class ParMeasureColdLatency {
    private static final Logger logger = LoggerFactory.getLogger(ParMeasureColdLatency.class);

    private static void syntaxError() {
        System.err.println("SYNTAX: MeasureLatency config.toml [decay]");
        System.exit(2);
    }

    private static final SplittableRandom rand = new SplittableRandom(0);

    public static void main(String[] args) throws Exception {
        File configFile;
        if (args.length < 1 || !(configFile = new File(args[0])).isFile()) {
            syntaxError();
            return;
        }
        Configuration conf = new Configuration(configFile);
        String decay;
        if (conf.getDecayFunctions().size() == 1) {
            decay = conf.getDecayFunctions().get(0);
        } else if (args.length >= 2) {
            decay = args[1];
        } else {
            syntaxError();
            return;
        }
        int nShards = conf.getNShards();
        long nStreams = conf.getNStreams();
        long nStreamsPerShard = conf.getNStreamsPerShard();

        Workload workload = conf.getWorkloadGenerator().generate(conf.getTstart(), conf.getTend());
        String[] groups = workload.keySet().toArray(new String[0]);
        Map<String, Statistics> groupStats = new LinkedHashMap<>();
        for (String group : workload.keySet()) {
            groupStats.put(group, new Statistics(true));
        }
        Statistics globalStats = new Statistics(true);
        for (List<Workload.Query> groupQueries : workload.values()) {
            for (Workload.Query q : groupQueries) {
                q.streamID = rand.nextInt(0, conf.getNStreams());
            }
        }
        List<Pair<String, Workload.Query>> shuffle = new ArrayList<>();
        while (!workload.isEmpty()) {
            String group = groups[rand.nextInt(0, groups.length)];
            if (!workload.containsKey(group)) continue;
            List<Workload.Query> groupQueries = workload.get(group);
            Workload.Query q = groupQueries.remove(rand.nextInt(0, groupQueries.size()));
            q.streamID = rand.nextLong(0, nStreams);
            shuffle.add(new Pair<>(group, q));
            if (groupQueries.isEmpty()) workload.remove(group);
        }

        SummaryStore.Options storeOptions = new SummaryStore.Options()
                .setReadOnly(true)
                .setCacheSizePerStream(conf.getWindowCacheSize());
        SummaryStore[] stores = new SummaryStore[conf.getNShards()];
        try {
            for (int i = 0; i < stores.length; ++i) {
                stores[i] = new SummaryStore(conf.getStoreDirectory(decay) + ".shard" + i, storeOptions);
            }

            int N = shuffle.size();
            for (int n = 0; n < N; ++n) {
                if (n % 10 == 0) {
                    logger.info("Processed {} out of {} queries", n, N);
                }
                Configuration.dropKernelCaches();
                Pair<String, Workload.Query> p = shuffle.get(n);
                String group = p.getFirst();
                Workload.Query q = p.getSecond();

                Object[] params = q.params;
                if (params == null || params.length == 0) {
                    params = new Object[]{0.95d};
                } else {
                    Object[] newParams = new Object[params.length + 1];
                    System.arraycopy(params, 0, newParams, 0, params.length);
                    newParams[params.length] = 0.95d;
                    params = newParams;
                }
                long ts = System.currentTimeMillis();
                try {
                    stores[(int) q.streamID / nShards].query(q.streamID % nStreamsPerShard, q.l, q.r, q.operatorNum, params);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                long te = System.currentTimeMillis();
                double timeS = (te - ts) / 1000d;
                groupStats.get(group).addObservation(timeS);
                globalStats.addObservation(timeS);
                System.out.println(timeS);
            }
        } finally {
            for (SummaryStore store : stores) {
                if (store != null) store.close();
            }
        }

        String outPrefix = FilenameUtils.removeExtension(configFile.getAbsolutePath());
        try (BufferedWriter br = Files.newBufferedWriter(Paths.get(outPrefix + ".cold.tsv"))) {
            br.write(STATS_HEADER + "\n");
            for (String group : groups) {
                Statistics stats = groupStats.get(group);
                printStats(br, group, stats);
                stats.writeCDF(outPrefix + "." + group.replaceAll("\\s+", "_") + ".cold.cdf");
            }
            printStats(br, "ALL\tALL\tALL", globalStats);
            globalStats.writeCDF(outPrefix + ".cold.cdf");
        }
    }

    private static final String STATS_HEADER = "#query\tage class\tlength class\t"
            + "latency:p0\tlatency:mean\tlatency:p50\tlatency:p95\tlatency:p99\tlatency:p99.9\tlatency:p100";

    private static void printStats(BufferedWriter br, String group, Statistics stats) throws IOException {
        br.write(group);
        br.write("\t" + stats.getQuantile(0));
        br.write("\t" + stats.getMean());
        br.write("\t" + stats.getQuantile(0.5));
        br.write("\t" + stats.getQuantile(0.95));
        br.write("\t" + stats.getQuantile(0.99));
        br.write("\t" + stats.getQuantile(0.999));
        br.write("\t" + stats.getQuantile(1));
        br.write("\n");
    }
}
