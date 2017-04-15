package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.ResultError;
import com.samsung.sra.DataStore.SummaryStore;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Run the configured workload on all decays and print query answers. Does not test against true (enumerated) answers;
 * for that use PopulateWorkload + RunComparison instead
 */
public class RunWorkload {
    public static void main(String[] args) throws Exception {
        File configFile;
        if (args.length != 1 || !(configFile = new File(args[0])).isFile()) {
            System.err.println("SYNTAX: PopulateData config.toml");
            System.exit(2);
            return;
        }
        Configuration conf = new Configuration(configFile);

        Workload workload = conf.getWorkloadGenerator().generate(conf.getTstart(), conf.getTend());
        System.out.println("#decay\tgroup\tt0\tt1\tresult\terror");
        for (String decay: conf.getDecayFunctions()) {
            try (SummaryStore store = new SummaryStore(conf.getStorePrefix(decay), conf.getWindowCacheSize())) {
                for (Map.Entry<String, List<Workload.Query>> entry: workload.entrySet()) {
                    String group = entry.getKey();
                    for (Workload.Query q: entry.getValue()) {
                        ResultError re = (ResultError) store.query(0L, q.l, q.r, q.operatorNum, q.params);
                        System.out.printf("%s\t%s\t%d\t%d\t%s\t%s\n", decay, group, q.l, q.r, re.result, re.error);
                    }
                }
            }
        }
    }
}
