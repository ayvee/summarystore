package com.samsung.sra.experiments;

import com.samsung.sra.datastore.SummaryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

public class ParMeasureLatency {
    private static final long streamID = 0;
    private static final Logger logger = LoggerFactory.getLogger(ParMeasureLatency.class);

    private static void syntaxError() {
        System.err.println("SYNTAX: MeasureLatency config.toml decay");
        System.exit(2);
    }

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

        Workload wl = conf.getWorkloadGenerator().generate(conf.getTstart(), conf.getTend());
        Statistics stats = new Statistics(true);
        SummaryStore.Options storeOptions = new SummaryStore.Options()
                .setReadOnly(true)
                .setCacheSizePerStream(conf.getWindowCacheSize());
        conf.dropKernelCachesIfNecessary();
        SummaryStore[] stores = new SummaryStore[conf.getNShards()];
        for (int i = 0; i < stores.length; ++i) {
            stores[i] = new SummaryStore(conf.getStoreDirectory(decay) + ".shard" + i, storeOptions);
        }

        try (SummaryStore store = new SummaryStore(conf.getStoreDirectory(decay), storeOptions)) {
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
