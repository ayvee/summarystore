package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.*;
import org.apache.commons.math3.distribution.ZipfDistribution;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

public class VaryQueries {
    private static final StreamID sid = new StreamID(0);
    private static Random rand = new Random();
    private static Runtime runtime = Runtime.getRuntime();
    private static final int N = 100000;
    private static String prefix = "/tmp/tds_";
    private static String queryLogFile = "qlog.tsv";

    public static void main(String[] args) {
        try {
            BufferedWriter logWriter = Files.newBufferedWriter(Paths.get(queryLogFile));
            runtime.exec(new String[]{"sh", "-c", "rm -rf " + prefix + "*"}).waitFor();
            LinkedHashMap<String, DataStore> datastores = new LinkedHashMap<String, DataStore>();
            //int[] Bs = {1, 2, 4, 8, 16, 32, 64, 0, 128};
            //int[] Bs = {2048, 1024, 0, 512, 256, 128, 64, 32, 16, 8, 4, 2, 1};
            int[] Bs = {16384, 4546, 0, 4196, 1024, 256, 64, 16, 4, 1};
            //int[] Bs = {4546, 0, 1};
            for (int B: Bs) {
                if (B == 1) {
                    String name = "enumeration";
                    datastores.put(name, new EnumeratedStore(prefix + name));
                } else if (B == 0) {
                    String name = "summarystore";
                    datastores.put(name, new SummaryStore(prefix + name, new ExponentialWBMHBucketMerger(2)));
                } else {
                    String name = "linearstore(" + B + ")";
                    datastores.put(name, new SummaryStore(prefix + name, new LinearBucketMerger(B)));
                }
            }

            for (DataStore ds: datastores.values()) {
                ds.registerStream(sid);
            }
            for (int n = 0; n < N; ++n) {
                int v = 1 + rand.nextInt(100);
                for (DataStore ds: datastores.values()) {
                    ds.append(sid, v, false, false);
                }
            }

            Map<String, Long> storeSizes = new HashMap<String, Long>();
            for (Map.Entry<String, DataStore> entry: datastores.entrySet()) {
                String name = entry.getKey();
                DataStore ds = entry.getValue();
                long size = ds.getStoreSizeInBytes();
                storeSizes.put(name, size);
            }

            System.out.println("#s\tstore name and size\tinflation\tquery time msec");
            logWriter.write("#s\tstore name and size\tl\tr\tla\tra\tcount\ttrueCount\tquery time msec\n");
            double[] svals = {1e-6, 0.25, 0.5, 0.75, 1, 1.25, 1.5, 1.75, 2};
            for (double s: svals) {
                ZipfDistribution zipf = new ZipfDistribution(N, s);
                Map<String, Statistics> countInflations = new HashMap<String, Statistics>();
                Map<String, Statistics> queryTimes = new HashMap<String, Statistics>();
                for (String name: datastores.keySet()) {
                    countInflations.put(name, new Statistics(true));
                    queryTimes.put(name, new Statistics(true));
                }
                for (int q = 0; q < 10000; ++q) {
                    int v0 = N - zipf.sample(), v1 = N - zipf.sample();
                    int l = Math.min(v0, v1), r = Math.max(v0, v1);
                    Map<String, Integer> counts = new HashMap<String, Integer>();
                    Map<String, Double> queryTimesMS = new HashMap<String, Double>();
                    Double trueCount = null;
                    for (Map.Entry<String, DataStore> entry: datastores.entrySet()) {
                        String name = entry.getKey();
                        DataStore ds = entry.getValue();
                        Integer count;
                        Double queryTimeMS;
                        int la, ra;
                        if (ds instanceof SummaryStore) {
                            SummaryStore tds = (SummaryStore)ds;
                            la = tds.getLeftAlignmentPoint(sid, l);
                            ra = tds.getRightAlignmentPoint(sid, r);
                            long t0 = System.nanoTime();
                            count = (Integer)ds.query(sid, Bucket.QUERY_COUNT, l, r);
                            long te = System.nanoTime();
                            queryTimeMS = (te - t0) / 1e6d;
                        } else {
                            la = l;
                            ra = r;
                            //count = r - l + 1;
                            long t0 = System.nanoTime();
                            count = (Integer)ds.query(sid, Bucket.QUERY_COUNT, l, r);
                            long te = System.nanoTime();
                            assert count == r - l + 1;
                            queryTimeMS = (te - t0) / 1e6d;
                            trueCount = (double)count;
                        }
                        queryTimesMS.put(name, queryTimeMS);
                        counts.put(name, count);
                        logWriter.write(s + "\t" + l + "\t" + r + "\t" + la + "\t" + ra + "\t" + count + "\t" + queryTimeMS + "\n");
                    }
                    assert trueCount != null;
                    for (String name: datastores.keySet()) {
                        countInflations.get(name).addObservation(counts.get(name) / trueCount);
                        queryTimes.get(name).addObservation(queryTimesMS.get(name));
                    }
                }

                for (String name: datastores.keySet()) {
                    System.out.println(s + "\t" +
                            name + "\\n" + storeSizes.get(name) + " bytes\t" +
                            countInflations.get(name).getErrorbars() + "\t" +
                            queryTimes.get(name).getErrorbars());
                }
            }
            logWriter.close();

            for (Map.Entry<String, DataStore> entry: datastores.entrySet()) {
                DataStore ds = entry.getValue();
                ds.close();
                /*String name = entry.getKey();
                int sizeKB = getSizeKB(prefix + name);
                System.err.println(name + "\t" + sizeKB);*/
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
