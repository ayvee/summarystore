/*
* Copyright 2016 Samsung Research America. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.samsung.sra.experiments;

import com.samsung.sra.datastore.SummaryStore;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class ParMeasureLatency {
    private static final Logger logger = LoggerFactory.getLogger(ParMeasureLatency.class);

    private static void syntaxError() {
        System.err.println("SYNTAX: MeasureLatency config.toml [decay]");
        System.exit(2);
    }

    private static final SplittableRandom rand = new SplittableRandom(0);

    public static void main(String[] args) throws Exception {
        File configFile;
        if (args.length < 1 || !(configFile = new File(args[0])).isFile()) {
            syntaxError();
            return;
        }
        Configuration conf = new Configuration(configFile);
        String decay;
        if (conf.getDecayFunctions().size() == 1) {
            decay = conf.getDecayFunctions().get(0);
        } else if (args.length >= 2) {
            decay = args[1];
        } else {
            syntaxError();
            return;
        }
        int nShards = conf.getNShards();
        long nStreams = conf.getNStreams();
        long nStreamsPerShard = conf.getNStreamsPerShard();

        Workload workload = conf.getWorkloadGenerator().generate(conf.getTstart(), conf.getTend());
        String[] groups = workload.keySet().toArray(new String[0]);
        Map<String, Statistics> groupStats = new LinkedHashMap<>();
        for (String group : workload.keySet()) {
            groupStats.put(group, new Statistics(true));
        }
        Statistics globalStats = new Statistics(true);
        for (List<Workload.Query> groupQueries : workload.values()) {
            for (Workload.Query q : groupQueries) {
                q.streamID = rand.nextInt(0, conf.getNStreams());
            }
        }
        List<Pair<String, Workload.Query>> shuffle = new ArrayList<>();
        while (!workload.isEmpty()) {
            String group = groups[rand.nextInt(0, groups.length)];
            if (!workload.containsKey(group)) continue;
            List<Workload.Query> groupQueries = workload.get(group);
            Workload.Query q = groupQueries.remove(rand.nextInt(0, groupQueries.size()));
            q.streamID = rand.nextLong(0, nStreams);
            shuffle.add(new Pair<>(group, q));
            if (groupQueries.isEmpty()) workload.remove(group);
        }

        SummaryStore.StoreOptions storeOptions = new SummaryStore.StoreOptions()
                .setReadOnly(true)
                .setReadCacheSizePerStream(conf.getWindowCacheSize());
        conf.dropKernelCachesIfNecessary();
        SummaryStore[] stores = new SummaryStore[conf.getNShards()];
        try {
            for (int i = 0; i < stores.length; ++i) {
                stores[i] = new SummaryStore(conf.getStoreDirectory(decay) + ".shard" + i, storeOptions);
            }

            int N = shuffle.size();
            AtomicInteger n = new AtomicInteger(0);
            // FIXME: config param means something else, should really add another
            Stream<Pair<String, Workload.Query>> stream = conf.isWorkloadParallelismEnabled()
                    ? shuffle.parallelStream() : shuffle.stream();
            stream.forEach(p -> {
                {
                    int nv = n.getAndIncrement();
                    if (nv % 10 == 0) {
                        logger.info("Processed {} out of {} queries", nv, N);
                    }
                }
                String group = p.getFirst();
                Workload.Query q = p.getSecond();

                Object[] params = q.params;
                if (params == null || params.length == 0) {
                    params = new Object[]{0.95d};
                } else {
                    Object[] newParams = new Object[params.length + 1];
                    System.arraycopy(params, 0, newParams, 0, params.length);
                    newParams[params.length] = 0.95d;
                    params = newParams;
                }
                long ts = System.currentTimeMillis();
                try {
                    stores[(int) q.streamID / nShards].query(q.streamID % nStreamsPerShard, q.l, q.r, q.operatorNum, params);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                long te = System.currentTimeMillis();
                double timeS = (te - ts) / 1000d;
                System.out.printf("%s\t%f\n", group, timeS);
                groupStats.get(group).addObservation(timeS);
                globalStats.addObservation(timeS);
            });
        } finally {
            for (SummaryStore store : stores) {
                if (store != null) store.close();
            }
        }

        String outPrefix = FilenameUtils.removeExtension(configFile.getAbsolutePath());
        try (BufferedWriter bw = Files.newBufferedWriter(Paths.get(outPrefix + ".tsv"))) {
            bw.write(STATS_HEADER + "\n");
            for (String group : groups) {
                Statistics stats = groupStats.get(group);
                printStats(bw, group, stats);
                stats.writeCDF(outPrefix + "." + group.replaceAll("\\s+", "_") + ".cdf");
            }
            printStats(bw, "ALL\tALL\tALL", globalStats);
            globalStats.writeCDF(outPrefix + ".cdf");
        }
    }

    private static final String STATS_HEADER = "#query\tage class\tlength class\t"
            + "latency:p0\tlatency:mean\tlatency:p50\tlatency:p95\tlatency:p99\tlatency:p99.9\tlatency:p100";

    private static void printStats(BufferedWriter br, String group, Statistics stats) throws IOException {
        br.write(group);
        br.write("\t" + stats.getQuantile(0));
        br.write("\t" + stats.getMean());
        br.write("\t" + stats.getQuantile(0.5));
        br.write("\t" + stats.getQuantile(0.95));
        br.write("\t" + stats.getQuantile(0.99));
        br.write("\t" + stats.getQuantile(0.999));
        br.write("\t" + stats.getQuantile(1));
        br.write("\n");
    }
}
