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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class GenerateWorkload {
    private static Logger logger = LoggerFactory.getLogger(GenerateWorkload.class);

    public static class Query<R> implements Serializable {
        long l, r;
        int operatorNum;
        Object[] params;
        R trueAnswer;

        public Query(long l, long r, int operatorNum, Object[] params, R trueAnswer) {
            this.l = l;
            this.r = r;
            this.operatorNum = operatorNum;
            this.params = params;
            this.trueAnswer = trueAnswer;
        }

        @Override
        public String toString() {
            return "trueAnswer[" + l + ", " + r + "] = " + trueAnswer;
        }
    }

    public static ConcurrentHashMap<AgeLengthClass, List<Query<Long>>> generate(
            long T, InterarrivalDistribution I, ValueDistribution V, long R,
            int A, int L, int Q) throws StreamException {
        ConcurrentHashMap<AgeLengthClass, List<Query<Long>>> ret = new ConcurrentHashMap<>();
        ArrayList<LongRange> queryIntervals = new ArrayList<>();
        ArrayList<Query<Long>> allQueries = new ArrayList<>();

        {
            Random random = new Random(0);
            List<AgeLengthClass> alClasses = AgeLengthSampler.getAgeLengthClasses(T, T, A, L);
            for (AgeLengthClass alClass: alClasses) {
                List<Query<Long>> thisClassQueries = new ArrayList<>();
                for (int q = 0; q < Q; ++q) {
                    Pair<Long> al = alClass.sample(random);
                    long age = al.first(), length = al.second();
                    long l = T - length + 1 - age, r = T - age;
                    if (0 <= l && r < T) {
                        Query<Long> query = new Query<>(l, r, 0, null, 0L);
                        thisClassQueries.add(query);
                        allQueries.add(query);
                        queryIntervals.add(new LongRange(l + ":" + r, l, true, r, true));
                    }
                }
                ret.put(alClass, thisClassQueries);
            }
        }

        computeTrueAnswers(T, new StreamGenerator(I, V, R), queryIntervals, allQueries);

        return ret;
    }

    private static void computeTrueAnswers(long T, StreamGenerator streamGenerator,
                                           ArrayList<LongRange> intervals, ArrayList<Query<Long>> queries) {
        assert intervals.size() == queries.size();
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
                Query<Long> q = queries.get(matchedIndexes[i]);
                ++q.trueAnswer;
            }
        });
    }

    public static void test() {
        long T = 10_000_000;
        int Q = 1000;
        StreamGenerator streamGenerator = new StreamGenerator(new FixedInterarrival(1), new UniformValues(0, 100), 0);

        ArrayList<LongRange> intervals = new ArrayList<>();
        ArrayList<Query<Long>> queries = new ArrayList<>();
        Random random = new Random(0);
        for (int q = 0; q < Q; ++q) {
            long a = Math.floorMod(random.nextLong(), T), b = Math.floorMod(random.nextLong(), T);
            long l = Math.min(a, b), r = Math.max(a, b);
            intervals.add(new LongRange(l + ":" + r, l , true, r, true));
            queries.add(new Query<>(l, r, 0, null, 0L));
        }
        computeTrueAnswers(T, streamGenerator, intervals, queries);
        for (Query<Long> q: queries) {
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

        ConcurrentHashMap<AgeLengthClass, List<Query<Long>>> workload = generate(T, interarrivals, values, R, A, L, Q);
        String outfile = String.format("%s/%sT%d.I%s.V%s.R%d.A%d.L%d.Q%d.workload", outdir, prefix, T, I, V, R, A, L, Q);
        try (FileOutputStream fos = new FileOutputStream(outfile)) {
            SerializationUtils.serialize(workload, fos);
        }
    }
}
