package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.SummaryStore;

import java.io.File;

public class PrintErrorTrend {
    private static final long streamID = 0L;

    public static void main(String[] args) throws Exception {
        File configFile;
        if (args.length != 2 || !(configFile = new File(args[0])).isFile()) {
            System.err.println("SYNTAX: PopulateData config.toml decayFunction");
            System.exit(2);
            return;
        }
        Configuration conf = new Configuration(configFile);
        String decay = args[1];

        try (SummaryStore store = new SummaryStore(conf.getStorePrefix(decay), conf.getBucketCacheSize())) {
            store.printBucketState(streamID, true);
        }
    }
}
