package com.samsung.sra.experiments;

import com.samsung.sra.datastore.SummaryStore;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

public class ParMeasureLatency {
    private static final Logger logger = LoggerFactory.getLogger(ParMeasureLatency.class);

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

        Workload workload = conf.getWorkloadGenerator().generate(conf.getTstart(), conf.getTend());
        String[] groups = workload.keySet().toArray(new String[0]);
        Map<String, Statistics> groupStats = new LinkedHashMap<>();
        for (String group : workload.keySet()) {
            groupStats.put(group, new Statistics());
        }
        Statistics globalStats = new Statistics(true);
        for (List<Workload.Query> groupQueries : workload.values()) {
            for (Workload.Query q : groupQueries) {
                q.streamID = rand.nextInt(0, conf.getNStreams());
            }
        }

        SummaryStore.Options storeOptions = new SummaryStore.Options()
                .setReadOnly(true)
                .setCacheSizePerStream(conf.getWindowCacheSize());
        conf.dropKernelCachesIfNecessary();
        SummaryStore[] stores = new SummaryStore[conf.getNShards()];
        try {
            for (int i = 0; i < stores.length; ++i) {
                stores[i] = new SummaryStore(conf.getStoreDirectory(decay) + ".shard" + i, storeOptions);
            }

            int nShards = conf.getNShards();
            long nStreams = conf.getNStreams();
            long nStreamsPerShard = conf.getNStreamsPerShard();
            int N = workload.values().stream().mapToInt(List::size).sum();
            int n = 0;
            while (!workload.isEmpty()) {
                if (n++ % 10 == 0) logger.info("Processed {} out of {} queries", n, N);
                // synchronized (workload) {
                String group = groups[rand.nextInt(0, groups.length)];
                List<Workload.Query> groupQueries = workload.get(group);
                Workload.Query q = groupQueries.remove(rand.nextInt(0, groupQueries.size()));
                q.streamID = rand.nextLong(0, nStreams);
                if (groupQueries.isEmpty()) workload.remove(group);
                // }

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
                stores[(int) q.streamID / nShards].query(q.streamID % nStreamsPerShard, q.l, q.r, q.operatorNum, params);
                long te = System.currentTimeMillis();
                double timeS = (te - ts) / 1000d;
                groupStats.get(group).addObservation(timeS);
                globalStats.addObservation(timeS);
            }

            String outPrefix = FilenameUtils.removeExtension(configFile.getAbsolutePath());
            System.out.println("#query\tage class\tlength class\tlatency:p0\tlatency:mean\tlatency:p50\tlatency:p95\tlatency:p99\tlatency:p99.9\tlatency:p100");
            for (String group : groups) {
                Statistics stats = groupStats.get(group);
                System.out.print(group);
                System.out.print("\t" + stats.getQuantile(0));
                System.out.print("\t" + stats.getMean());
                System.out.print("\t" + stats.getQuantile(0.5));
                System.out.print("\t" + stats.getQuantile(0.95));
                System.out.print("\t" + stats.getQuantile(0.99));
                System.out.print("\t" + stats.getQuantile(0.999));
                System.out.print("\t" + stats.getQuantile(1));
                System.out.println();
                stats.writeCDF(outPrefix + "." + group + ".cdf");
            }
            globalStats.writeCDF(outPrefix + ".cdf");
        } finally {
            for (SummaryStore store : stores) {
                if (store != null) store.close();
            }
        }
    }
}
