package com.samsung.sra.DataStoreExperiments;

import com.changingbits.Builder;
import com.changingbits.LongRange;
import com.changingbits.LongRangeMultiSet;
import com.samsung.sra.DataStore.StreamException;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.*;
import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
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
        StreamGenerator streamGenerator = new RandomStreamGenerator(new FixedInterarrival(1), new UniformValues(0, 100), 0);

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

    public static void main(String[] args) throws StreamException, IOException {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("CompareDecayFunctions", false).
                description("compute statistics for each decay function and age length class, " +
                        "and optionally print weighted stats if a weight function and metric are specified").
                defaultHelp(true);
        ArgumentType<Long> CommaSeparatedLong = (ArgumentParser argParser, Argument arg, String value) ->
                Long.valueOf(value.replace(",", ""));
        parser.addArgument("outdir").help("output directory");
        parser.addArgument("T").help("size of stream").type(CommaSeparatedLong);
        parser.addArgument("operator").nargs("+").help("window operators [" + CLIParser.getValidOperators() + "]");
        parser.addArgument("-I")
                .help("interarrival distribution [" + CLIParser.getValidInterarrivalDistributions() + "]")
                .setDefault("fixed1");
        parser.addArgument("-V")
                .help("value distribution [" + CLIParser.getValidValueDistributions() + "]")
                .setDefault("uniform0,100");
        parser.addArgument("-R").help("stream generator RNG seed").type(Long.class).setDefault(0L);
        parser.addArgument("-A").help("number of age classes").type(int.class).setDefault(8);
        parser.addArgument("-L").help("number of length classes").type(int.class).setDefault(8);
        parser.addArgument("-Q").help("number of random queries to run per class").type(int.class).setDefault(1000);
        parser.addArgument("-prefix").help("optional prefix to add to every input/output file").setDefault("");

        String outdir;
        long T;
        String I, V;
        InterarrivalDistribution interarrivals;
        ValueDistribution values;
        long R;
        int A, L, Q;
        String prefix;
        try {
            Namespace parsed = parser.parseArgs(args);
            outdir = parsed.get("outdir");
            T = parsed.get("T");
            if (T <= 0) {
                throw new IllegalArgumentException("T should be positive");
            }
            I = parsed.get("I");
            interarrivals = CLIParser.parseInterarrivalDistribution(I);
            V = parsed.get("V");
            values = CLIParser.parseValueDistribution(V);
            R = parsed.get("R");
            A = parsed.get("A");
            L = parsed.get("L");
            Q = parsed.get("Q");
            prefix = parsed.get("prefix");
        } catch (ArgumentParserException | IllegalArgumentException e) {
            System.err.println("ERROR: " + e.getMessage());
            parser.printHelp(new PrintWriter(System.err, true));
            System.exit(2);
            return;
        }

        StreamGenerator streamGenerator = new RandomStreamGenerator(interarrivals, values, R);
        WorkloadGenerator<Long> workloadGenerator = new RandomWorkloadGenerator(A, L, Q);
        Workload<Long> workload = workloadGenerator.generate(T);
        computeTrueAnswers(T, streamGenerator, workload);
        String outfile = String.format("%s/%sT%d.I%s.V%s.R%d.A%d.L%d.Q%d.workload", outdir, prefix, T, I, V, R, A, L, Q);
        try (FileOutputStream fos = new FileOutputStream(outfile)) {
            SerializationUtils.serialize(workload, fos);
        }
    }
}
