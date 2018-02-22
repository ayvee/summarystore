package com.samsung.sra.experiments;

import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class LRMSPopulateWorkloadTest {
    @Test
    public void computeTrueAnswers() throws Exception {
        File configFile = File.createTempFile("test-workload", "toml");
        configFile.deleteOnExit();
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(configFile.getAbsolutePath()))) {
            writer.write("directory = \"/tmp\"\n"
                    + "[data]\n"
                    + "tstart = 0\n"
                    + "tend = 10_000\n"
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
        List<Workload.Query> queries = new ArrayList<>();
        workload.put("", queries);
        Random random = new Random(0);
        for (int q = 0; q < Q; ++q) {
            long a = Math.floorMod(random.nextLong(), T), b = Math.floorMod(random.nextLong(), T);
            long l = Math.min(a, b), r = Math.max(a, b);
            queries.add(new Workload.Query(Workload.Query.Type.COUNT, l, r, 0, null));
        }
        LRMSPopulateWorkload.computeTrueAnswers(conf, workload);
        for (Workload.Query q : queries) {
            assertEquals(q.r - q.l + 1, q.trueAnswer.get());
        }
    }
}