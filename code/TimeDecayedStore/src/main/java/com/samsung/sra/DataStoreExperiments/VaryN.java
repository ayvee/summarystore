package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.*;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.*;

public class VaryN {
    private static final StreamID sid = new StreamID(0);
    private static final String tdLoc = "/tmp/tdstore", eLoc = "/tmp/estore";
    private static final Random rand = new Random();
    private static final Runtime runtime = Runtime.getRuntime();
    public static final int Q = 10000;
    public static final int V = 100;

    public static void main(String[] args) {
        try {
            doExperiment(10000); // warmup
            System.out.println("#N\tTDS size KB\tES size KB\tTDS append latency ms\tES append latency ms\tCount inflation\tSum inflation\tTDS query latency ms\tES query latency ms");
            for (int N = 10; N <= 100000000; N *= 10) {
                System.out.println(doExperiment(N));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String doExperiment(int N) throws InterruptedException, IOException, RocksDBException, StreamException, LandmarkEventException, QueryException {
        runtime.exec(new String[]{"rm", "-rf", tdLoc, eLoc}).waitFor();
        TimeDecayedStore tdStore = new TimeDecayedStore(tdLoc, new WBMHBucketMerger(2));
        EnumeratedStore eStore = new EnumeratedStore(eLoc);

        Statistics tdAppendTime = new Statistics(false), eAppendTime = new Statistics(false);
        Statistics countInflation = new Statistics(true), sumInflation = new Statistics(true);
        Statistics tdQueryTime = new Statistics(true), eQueryTime = new Statistics(true);

        tdStore.registerStream(sid);
        eStore.registerStream(sid);
        for (int n = 0; n < N; ++n) {
            int v = 1 + rand.nextInt(V);
            doAppend(tdStore, v, tdAppendTime);
            doAppend(eStore, v, eAppendTime);
        }

        for (int q = 0; q < Q; ++q) {
            // querying EnumeratedStore takes too long for large N, so we do it a few times
            // to get a latency estimate and then cheat on the rest of the queries
            boolean fakeCount = N > 100000 && q > 9;
            int v0 = rand.nextInt(N), v1 = rand.nextInt(N);
            int l = Math.min(v0, v1), r = Math.max(v0, v1);
            doQuery(tdStore, eStore, l, r, countInflation, sumInflation, tdQueryTime, eQueryTime, fakeCount);
        }

        tdStore.close();
        eStore.close();

        int tdsSizeKB = getSizeKB(tdLoc), esSizeKB = getSizeKB(eLoc);

        return N + "\t" +
                tdsSizeKB + "\t" + esSizeKB + "\t" +
                tdAppendTime.getErrorbars() + "\t" + eAppendTime.getErrorbars() + "\t" +
                countInflation.getErrorbars() + "\t" + sumInflation.getErrorbars() + "\t" +
                tdQueryTime.getErrorbars() + "\t" + eQueryTime.getErrorbars();
    }

    private static void doAppend(DataStore ds, int v, Statistics timeStats) throws StreamException, RocksDBException, LandmarkEventException {
        List<FlaggedValue> val = Collections.singletonList(new FlaggedValue(v));
        long t0 = System.nanoTime();
        ds.append(sid, val);
        long te = System.nanoTime();
        timeStats.addObservation((te - t0) / 1e6d);
    }

    private static void doQuery(
            TimeDecayedStore tdStore, EnumeratedStore eStore,
            int l, int r,
            Statistics countInflation, Statistics sumInflation,
            Statistics tdQueryTime, Statistics eQueryTime,
            boolean fakeCount) throws StreamException, QueryException, RocksDBException {
        double tdCount, eCount;
        //double tdSum, eSum;

        long tdT0 = System.nanoTime();
        tdCount = (Integer)tdStore.query(sid, Bucket.QUERY_COUNT, l, r);
        //tdSum = (Integer)tdStore.query(sid, Bucket.QUERY_SUM, l, r);
        long tdTe = System.nanoTime();
        tdQueryTime.addObservation((tdTe - tdT0) / 1e6d);

        if (!fakeCount) {
            long eT0 = System.nanoTime();
            eCount = (Integer) eStore.query(sid, Bucket.QUERY_COUNT, l, r);
            //eSum = (Integer)eStore.query(sid, Bucket.QUERY_SUM, l, r);
            long eTe = System.nanoTime();
            eQueryTime.addObservation((eTe - eT0) / 1e6d);
        } else {
            eCount = r - l + 1;
        }

        countInflation.addObservation(tdCount / eCount);
        //sumInflation.addObservation(tdSum / eSum);
    }

    private static int getSizeKB(String path) throws IOException, InterruptedException {
        Process p = runtime.exec(new String[]{"du", "-sk", path});
        Scanner scanner = new Scanner(p.getInputStream());
        return scanner.nextInt();
    }
}
