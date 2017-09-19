package com.samsung.sra.experiments;

import com.samsung.sra.datastore.SummaryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

public class MeasureLatency {
    private static final long streamID = 0;
    private static final Logger logger = LoggerFactory.getLogger(PopulateData.class);

    public static void main(String[] args) throws Exception {
        File configFile;
        if (args.length != 2 || !(configFile = new File(args[0])).isFile()) {
            System.err.println("SYNTAX: MeasureLatency config.toml decay");
            System.exit(2);
            return;
        }
        Configuration conf = new Configuration(configFile);
        String decay = args[1];

        Workload wl = conf.getWorkloadGenerator().generate(conf.getTstart(), conf.getTend());
        Statistics stats = new Statistics(true);
        try (SummaryStore store = new SummaryStore(conf.getStorePrefix(decay), new SummaryStore.Options()
                .setKeepReadIndexes(true)
                .setReadOnly(true)
                .setCacheSizePerStream(conf.getWindowCacheSize()))) {
            //store.warmupCache();
            for (Map.Entry<String, List<Workload.Query>> entry: wl.entrySet()) {
                System.out.println("Group " + entry.getKey());
                List<Workload.Query> queries = entry.getValue();
                queries.parallelStream().forEach(q -> {
                    try {
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
                        store.query(streamID, q.l, q.r, q.operatorNum, params);
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
