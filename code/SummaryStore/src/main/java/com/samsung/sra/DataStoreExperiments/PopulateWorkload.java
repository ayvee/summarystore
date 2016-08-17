package com.samsung.sra.DataStoreExperiments;

import com.changingbits.Builder;
import com.changingbits.LongRange;
import com.changingbits.LongRangeMultiSet;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PopulateWorkload {
    private static Logger logger = LoggerFactory.getLogger(PopulateWorkload.class);

    private static void computeTrueAnswers(long T, StreamGenerator streamGenerator, Workload<Long> workload) throws IOException {
        ArrayList<LongRange> intervals = new ArrayList<>();
        ArrayList<Workload.Query<Long>> queries = new ArrayList<>();
        for (List<Workload.Query<Long>> classQueries: workload.values()) {
            for (Workload.Query<Long> q: classQueries) {
                queries.add(q);
                intervals.add(new LongRange(q.l + ":" + q.r, q.l, true, q.r, true));
            }
        }
        int Q = queries.size();
        Builder builder = new Builder(intervals.toArray(new LongRange[Q]), 0, T);
        LongRangeMultiSet lrms = builder.getMultiSet(false, true);
        int[] matchedIndexes = new int[Q];

        streamGenerator.generate(T, (t, v) -> {
            if (t % 1_000_000 == 0) {
                logger.info("t = {}", t);
            }
            int matchCount = lrms.lookup(t, matchedIndexes);
            for (int i = 0; i < matchCount; ++i) {
                Workload.Query<Long> q = queries.get(matchedIndexes[i]);
                ++q.trueAnswer;
            }
        });
    }

    public static void test() throws IOException {
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
            queries.add(new Workload.Query<>(l, r, 0, null, 0L));
        }
        computeTrueAnswers(T, streamGenerator, workload);
        for (Workload.Query<Long> q: queries) {
            assert q.r - q.l + 1 == q.trueAnswer;
        }
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("PopulateWorkload", false).defaultHelp(true);
        parser.addArgument("conf", "config file").type(File.class);
        Configuration config = new Configuration(parser.parseArgs(args).get("conf"));

        long T = config.getT();
        try (StreamGenerator streamGenerator = config.getStreamGenerator()) {
            WorkloadGenerator<Long> workloadGenerator = config.getWorkloadGenerator();
            Workload<Long> workload = workloadGenerator.generate(T);
            computeTrueAnswers(T, streamGenerator, workload);
            try (FileOutputStream fos = new FileOutputStream(config.getWorkloadFile())) {
                SerializationUtils.serialize(workload, fos);
            }
        }
    }
}
