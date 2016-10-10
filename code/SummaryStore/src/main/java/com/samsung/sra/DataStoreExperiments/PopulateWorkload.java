package com.samsung.sra.DataStoreExperiments;

import com.changingbits.Builder;
import com.changingbits.LongRange;
import com.changingbits.LongRangeMultiSet;
import com.samsung.sra.DataStoreExperiments.Workload.Query;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;

public class PopulateWorkload {
    private static Logger logger = LoggerFactory.getLogger(PopulateWorkload.class);

    private static void computeTrueAnswers(Configuration conf, Workload workload) throws Exception {
        long T0 = conf.getTstart(), T1 = conf.getTend();
        ArrayList<LongRange> intervals = new ArrayList<>();
        ArrayList<Query> queries = new ArrayList<>();
        for (List<Query> classQueries: workload.values()) {
            for (Query q: classQueries) {
                queries.add(q);
                intervals.add(new LongRange(q.l + ":" + q.r, q.l, true, q.r, true));
            }
        }
        int Q = queries.size();
        Builder builder = new Builder(intervals.toArray(new LongRange[Q]), T0, T1);
        LongRangeMultiSet lrms = builder.getMultiSet(false, true);

        if (!conf.isWorkloadParallelismEnabled()) {
            int[] matchedIndexes = new int[Q];
            long[] N = {0};
            conf.getStreamGenerator().generate(T0, T1, (t, v) -> {
                if (++N[0] % 1_000_000 == 0) logger.info("t = {}", t);
                processDataPoint(queries, lrms, matchedIndexes, t, v);
            });
        } else {
            // Divide [0, N) into equal-size bins and process one bin per thread (where N = # of data points in stream)
            int nThreads = ForkJoinPool.getCommonPoolParallelism(); // by default = # CPU cores - 1
            assert nThreads > 0;
            logger.info("# threads = {}", nThreads);
            long N;
            {
                MutableLong nvals = new MutableLong(0L);
                try (StreamGenerator streamGenerator = conf.getStreamGenerator()) {
                    streamGenerator.generate(T0, T1, (t, v) -> nvals.increment());
                }
                N = nvals.toLong();
            }
            IntStream.range(0, nThreads).parallel().forEach(threadNum -> {
                // this thread will process values with count [Nleft, Nright)
                long Nleft = threadNum * (N / nThreads);
                long Nright = (threadNum != nThreads - 1) ? Nleft + (N / nThreads) : N;
                logger.info("Thread {}: [{}, {})", threadNum, Nleft, Nright);
                try (StreamGenerator streamGenerator = conf.getStreamGenerator()) {
                    MutableLong Ncurr = new MutableLong(0L);
                    int[] matchedIndexes = new int[Q];
                    streamGenerator.generate(T0, T1, (t, v) -> {
                        if (Nleft <= Ncurr.toLong() && Ncurr.toLong() < Nright) {
                            if (Ncurr.toLong() % 1_000_000 == 0) logger.info("t = {}", t);
                            processDataPoint(queries, lrms, matchedIndexes, t, v);
                        } else if (Ncurr.toLong() >= Nright) {
                            return;
                        }
                        Ncurr.increment();
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                logger.info("Thread {} done", threadNum);
            });
        }
    }

    /**
     * Process data point <t, v>. Lookup t in lrms, storing match output in matchedIndexes, then update all matching
     * queries' answers. Not thread-safe, assumes unique access to matchedIndexes.
     */
    private static void processDataPoint(List<Query> queries, LongRangeMultiSet lrms, int[] matchedIndexes,
                                         long t, Object[] v) {
        int matchCount = lrms.lookup(t, matchedIndexes);
        for (int i = 0; i < matchCount; ++i) {
            Query q = queries.get(matchedIndexes[i]);
            switch (q.queryType) {
                case COUNT:
                    q.trueAnswer.incrementAndGet();
                    break;
                case SUM:
                    q.trueAnswer.addAndGet((long) v[0]);
                    break;
                case CMS:
                    if (v[0].equals(q.params[0])) {
                        if (v.length > 1) {
                            q.trueAnswer.addAndGet((long) v[1]);
                        } else {
                            q.trueAnswer.incrementAndGet();
                        }
                    }
                    break;
            }
        }
    }

    public static void test() throws Exception {
        File configFile = File.createTempFile("test-workload", "toml");
        configFile.deleteOnExit();
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(configFile.getAbsolutePath()))) {
            writer.write(
                       "directory = \"/tmp\"\n"
                    + "[data]\n"
                    + "tstart = 0\n"
                    + "tend = 10_000_000\n"
                    + "stream-generator = \"RandomStreamGenerator\"\n"
                    + "interarrivals = {distribution = \"FixedDistribution\", value = 1}\n"
                    + "values = {distribution = \"FixedDistribution\", value = 1}\n"
                    + "[workload]\n"
                    + "enable-parallelism = true\n"
            );
        }
        Configuration conf = new Configuration(configFile);
        assert conf.getTstart() == 0;
        long T = conf.getTend();
        int Q = 1000;

        Workload workload = new Workload();
        List<Query> queries = new ArrayList<>();
        workload.put("", queries);
        Random random = new Random(0);
        for (int q = 0; q < Q; ++q) {
            long a = Math.floorMod(random.nextLong(), T), b = Math.floorMod(random.nextLong(), T);
            long l = Math.min(a, b), r = Math.max(a, b);
            queries.add(new Query(Query.Type.COUNT, l, r, 0, null));
        }
        computeTrueAnswers(conf, workload);
        for (Query q : queries) {
            if (q.r - q.l + 1 != q.trueAnswer.get()) {
                throw new RuntimeException("incorrect answer in query " + q);
            }
        }
        logger.info("Test succeeded");
    }

    public static void main(String[] args) throws Exception {
        //test(); System.exit(0);
        File configFile;
        if (args.length != 1 || !(configFile = new File(args[0])).isFile()) {
            System.err.println("SYNTAX: PopulateData config.toml");
            System.exit(2);
            return;
        }
        Configuration conf = new Configuration(configFile);

        if ((new File(conf.getWorkloadFile())).isFile()) {
            logger.warn("Workload file {} already exists, skipping generation", conf.getWorkloadFile());
            System.exit(1);
        }
        Workload workload = conf.getWorkloadGenerator().generate(conf.getTstart(), conf.getTend());
        computeTrueAnswers(conf, workload);
        try (FileOutputStream fos = new FileOutputStream(conf.getWorkloadFile())) {
            SerializationUtils.serialize(workload, fos);
        }
    }
}
