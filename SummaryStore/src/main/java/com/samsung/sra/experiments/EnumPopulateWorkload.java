package com.samsung.sra.experiments;

import com.samsung.sra.experiments.Workload.Query;
import it.unimi.dsi.fastutil.longs.Long2LongRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2LongSortedMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import org.apache.commons.lang3.SerializationUtils;
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
import java.util.stream.Stream;

public class EnumPopulateWorkload {
    private static Logger logger = LoggerFactory.getLogger(EnumPopulateWorkload.class);

    /** Working in batches, ingest portions of the enum trace into an in-memory tree, then update query answers for batch */
    private static void computeTrueAnswers(Configuration conf, Workload workload) throws Exception {
        Long2LongSortedMap data = new Long2LongRBTreeMap();
        long enumBatchSize = conf.getEnumBatchSize();
        RandomStreamIterator ris = conf.getStreamIterator();
        long N = 0;
        while (ris.hasNext()) {
            data.clear();
            long Ni = 0;
            while (ris.hasNext() && Ni < enumBatchSize) {
                if (N % 10_000_000 == 0) {
                    logger.info("Processed {} values", String.format("%,d", N));
                }
                ++Ni;
                ++N;
                data.put(ris.currT, ris.currV);
                ris.next();
            }
            logger.info("Starting query batch update");
            for (List<Query> classQueries: workload.values()) {
                Stream<Query> stream = classQueries.stream();
                if (conf.isWorkloadParallelismEnabled()) {
                    stream = stream.parallel();
                }
                stream.forEach(q -> processQuery(data, q));
            }
            logger.info("Query batch update complete");
        }
    }

    private static void processQuery(Long2LongSortedMap data, Query q) {
        LongIterator iter = data.subMap(q.l, q.r+1).values().iterator();
        if (q.queryType == Query.Type.BF) {
            if (q.trueAnswer.longValue() == 1) {
                return; // already true
            }
            long b = (long) q.params[0];
            while (iter.hasNext()) {
                if (b == iter.nextLong()) {
                    q.trueAnswer.set(1);
                    return;
                }
            }
        } else {
            long ans = 0;
            switch (q.queryType) {
                case COUNT:
                    while (iter.hasNext()) {
                        ans += 1;
                        iter.nextLong();
                    }
                    break;
                case SUM:
                    while (iter.hasNext()) {
                        ans += iter.nextLong();
                    }
                    break;
                case CMS:
                    long c = (long) q.params[0];
                    // FIXME: ignores two-param inserts to CMS (also specifying frequency)
                    while (iter.hasNext()) {
                        if (c == iter.nextLong()) {
                            ++ans;
                        }
                    }
                    break;
            }
            q.trueAnswer.addAndGet(ans);
        }
    }

    public static void test() throws Exception {
        File configFile = File.createTempFile("test-workload", "toml");
        configFile.deleteOnExit();
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(configFile.getAbsolutePath()))) {
            writer.write(
                       "directory = \"/tmp\"\n"
                    + "[data]\n"
                    + "dimensionality = 1\n"
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
        long T0 = conf.getTstart(), Te = conf.getTend();
        /*long partI = conf.getPartialGenI(), partN = conf.getPartialGenN();
        if (conf.getPartialGenN() != 1 && conf.getPartialGenI() < conf.getPartialGenN() - 1) {
            // generate workload we have only generated upto the current partI
            long binsize = (Te - T0) / partN;
            Te = T0 + partI * binsize + binsize - 1;
        }*/
        Workload workload = conf.getWorkloadGenerator().generate(T0, Te);
        computeTrueAnswers(conf, workload);
        try (FileOutputStream fos = new FileOutputStream(conf.getWorkloadFile())) {
            SerializationUtils.serialize(workload, fos);
        }
    }
}
