package com.samsung.sra.DataStoreExperiments;

import com.changingbits.Builder;
import com.changingbits.LongRange;
import com.changingbits.LongRangeMultiSet;
import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PopulateWorkload {
    private static Logger logger = LoggerFactory.getLogger(PopulateWorkload.class);

    private static void computeTrueAnswers(long T0, long T1, StreamGenerator streamGenerator, Workload workload) throws IOException {
        ArrayList<LongRange> intervals = new ArrayList<>();
        ArrayList<Workload.Query> queries = new ArrayList<>();
        for (List<Workload.Query> classQueries: workload.values()) {
            for (Workload.Query q: classQueries) {
                queries.add(q);
                intervals.add(new LongRange(q.l + ":" + q.r, q.l, true, q.r, true));
            }
        }
        int Q = queries.size();
        Builder builder = new Builder(intervals.toArray(new LongRange[Q]), T0, T1);
        LongRangeMultiSet lrms = builder.getMultiSet(false, true);
        int[] matchedIndexes = new int[Q];

        long[] N = {0};
        streamGenerator.generate(T0, T1, (t, v) -> {
            if (++N[0] % 1_000_000 == 0) logger.info("Processed {} data points", N[0]);
            int matchCount = lrms.lookup(t, matchedIndexes);
            for (int i = 0; i < matchCount; ++i) {
                Workload.Query q = queries.get(matchedIndexes[i]);
                switch (q.queryType) {
                    case COUNT:
                        ++q.trueAnswer;
                        break;
                    case SUM:
                        q.trueAnswer += (long) v[0];
                        break;
                    case CMS:
                        if (v[0].equals(q.params[0])) {
                            q.trueAnswer += v.length > 1 ? (long) v[1] : 1;
                        }
                        break;
                }
            }
        });
    }

    /*public static void test() throws IOException {
        long T = 10_000_000;
        int Q = 1000;
        StreamGenerator streamGenerator = new RandomStreamGenerator(new FixedDistribution(1), new UniformDistribution(0, 100), 0);

        Workload<Long> workload = new Workload<>();
        List<Workload.Query<Long>> queries = new ArrayList<>();
        workload.put("", queries);
        Random random = new Random(0);
        for (int q = 0; q < Q; ++q) {
            long a = Math.floorMod(random.nextLong(), T), b = Math.floorMod(random.nextLong(), T);
            long l = Math.min(a, b), r = Math.max(a, b);
            queries.add(new Workload.Query<>("count", l, r, 0, null, 0L));
        }
        computeTrueAnswers(T, streamGenerator, workload);
        for (Workload.Query<Long> q: queries) {
            assert q.r - q.l + 1 == q.trueAnswer;
        }
    }*/

    public static void main(String[] args) throws Exception {
        File configFile;
        if (args.length != 1 || !(configFile = new File(args[0])).isFile()) {
            System.err.println("SYNTAX: PopulateData config.toml");
            System.exit(2);
            return;
        }
        Configuration config = new Configuration(configFile);

        if ((new File(config.getWorkloadFile())).isFile()) {
            logger.warn("Workload file {} already exists, skipping generation", config.getWorkloadFile());
            System.exit(1);
        }
        long T0 = config.getTstart(), T1 = config.getTend();
        try (StreamGenerator streamGenerator = config.getStreamGenerator()) {
            WorkloadGenerator workloadGenerator = config.getWorkloadGenerator();
            Workload workload = workloadGenerator.generate(T0, T1);
            computeTrueAnswers(T0, T1, streamGenerator, workload);
            try (FileOutputStream fos = new FileOutputStream(config.getWorkloadFile())) {
                SerializationUtils.serialize(workload, fos);
            }
        }
    }
}
