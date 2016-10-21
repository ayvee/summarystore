package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.SummaryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

public class MeasureLatency {
    private static final long streamID = 0;
    private static final Logger logger = LoggerFactory.getLogger(PopulateData.class);

    public static void main(String[] args) throws Exception {
        File configFile;
        if (args.length != 1 || !(configFile = new File(args[0])).isFile()) {
            System.err.println("SYNTAX: MeasureLatency config.toml decay");
            System.exit(2);
            return;
        }
        Configuration conf = new Configuration(configFile);
        String decay = args[1];

        Workload wl = conf.getWorkloadGenerator().generate(conf.getTstart(), conf.getTend());
        Statistics stats = new Statistics();
        try (SummaryStore store = new SummaryStore(conf.getStorePrefix(decay), conf.getBucketCacheSize())) {
            store.warmupCache();
            for (List<Workload.Query> queries: wl.values()) {
                queries.forEach(q -> {
                    try {
                        long ts = System.currentTimeMillis();
                        store.query(streamID, q.l, q.r, q.operatorNum, q.params);
                        long te = System.currentTimeMillis();
                        stats.addObservation((te - ts) / 1000d);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
        stats.writeCDF("latency.cdf");
    }
}
