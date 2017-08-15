package com.samsung.sra.experiments;

import com.samsung.sra.datastore.ResultError;
import com.samsung.sra.datastore.SummaryStore;
import com.samsung.sra.experiments.Workload.Query;
import org.apache.commons.math3.util.Pair;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class PrintErrorTrend {
    private static final long streamID = 0L;
    private static final int operatorIndex = 0;

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
            try (SummaryStore store = new SummaryStore(conf.getStorePrefix(decay), true, true, conf.getWindowCacheSize())) {
                System.out.println("no time range specified, printing window state");
                store.printWindowState(streamID);
            }
            return;
        }
        long T0 = Long.parseLong(args[3]), T1 = Long.parseLong(args[4]);
        long numQueries = 100L;

        List<Query> queries = new ArrayList<>();
        long incr = (T1 - T0) / numQueries;
        for (long T = T0; T < T1 + incr; T += incr) {
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
            queries.add(new Query(queryType, T0, T <= T1 ? T : T1, operatorIndex, params));
        }
        Workload workload = new Workload();
        workload.put("all", queries);
        PopulateWorkload.computeTrueAnswers(conf, workload);
        try (SummaryStore store = new SummaryStore(conf.getStorePrefix(decay), true, true, conf.getWindowCacheSize())) {
            if (queryType == Query.Type.COUNT) {
                System.out.println("#length\ttrue answer\testimate\tCI left\tCI right");
            } else if (queryType == Query.Type.BF) {
                System.out.println("#length\ttrue answer\testimate\tFP");
            }
            for (Query q: queries) {
                ResultError re = (ResultError) store.query(streamID, q.l, q.r, q.operatorNum, q.params);
                if (q.queryType == Query.Type.COUNT) {
                    double estimate = (double) re.result;
                    Pair<Double, Double> ci = (Pair) re.error;
                    System.out.printf("%d\t%d\t%f\t%f\t%f\n", q.r - q.l + 1, q.trueAnswer.get(), estimate, ci.getFirst(), ci.getSecond());
                } else if (q.queryType == Query.Type.BF) {
                    long estimate = ((boolean) re.result) ? 1 : 0;
                    double fp = (double) re.error;
                    System.out.printf("%d\t%d\t%d\t%f\n", q.r - q.l + 1, q.trueAnswer.get(), estimate, fp);
                }
            }
        }
    }
}
