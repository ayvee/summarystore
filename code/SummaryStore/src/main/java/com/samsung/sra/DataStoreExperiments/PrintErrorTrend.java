package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.ResultError;
import com.samsung.sra.DataStore.SummaryStore;
import com.samsung.sra.DataStoreExperiments.Workload.Query;
import org.apache.commons.math3.util.Pair;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class PrintErrorTrend {
    private static final long streamID = 0L;
    private static final int operatorIndex = 0; // FIXME

    public static void main(String[] args) throws Exception {
        File configFile;
        if (args.length < 3 || !(configFile = new File(args[0])).isFile()) {
            System.err.println("SYNTAX: PrintErrorTrend config.toml decayFunction queryType T0 T1");
            System.exit(2);
            return;
        }
        Configuration conf = new Configuration(configFile);
        String decay = args[1];
        Query.Type queryType = Query.Type.valueOf(args[2].toUpperCase());
        if (args.length <= 3) {
            try (SummaryStore store = new SummaryStore(conf.getStorePrefix(decay), conf.getBucketCacheSize())) {
                System.out.println("no time range specified, printing bucket state");
                store.printBucketState(streamID, true);
            }
            return;
        }
        long T0 = Long.parseLong(args[3]), T1 = Long.parseLong(args[4]);
        long numQueries = 1000L;

        List<Query> queries = new ArrayList<>();
        for (long T = T0; T <= T1; T += (T1 - T0) / numQueries) {
            Object[] params;
            switch (queryType) {
                case COUNT:
                    params = new Object[]{0.95d};
                    break;
                case BF:
                    params = new Object[]{1L};
                    break;
                default:
                    throw new IllegalArgumentException("only support COUNT and BF");
            }
            queries.add(new Query(queryType, T0, T, operatorIndex, params));
        }
        Workload workload = new Workload();
        workload.put("all", queries);
        PopulateWorkload.computeTrueAnswers(conf, workload);
        try (SummaryStore store = new SummaryStore(conf.getStorePrefix(decay), conf.getBucketCacheSize())) {
            System.out.println("#length\ttrue answer\testimate\tCI width");
            for (Query q: queries) {
                ResultError re = (ResultError) store.query(streamID, q.l, q.r, q.operatorNum, q.params);
                if (q.queryType == Query.Type.COUNT) {
                    double estimate = (double) re.error;
                    Pair<Double, Double> ci = (Pair) re.error;
                    double ciWidth = ci.getSecond() - ci.getFirst();
                    System.out.printf("%d\t%d\t%f\t%f\n", q.r - q.l + 1, q.trueAnswer.get(), estimate, ciWidth);
                } else if (q.queryType == Query.Type.BF) {
                    long estimate = ((boolean) re.result) ? 1 : 0;
                    double ciWidth = (double) re.error;
                    System.out.printf("%d\t%d\t%d\t%f\n", q.r - q.l + 1, q.trueAnswer.get(), estimate, ciWidth);
                }
            }
        }
    }
}
