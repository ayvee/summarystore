package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.*;
import org.apache.commons.math3.distribution.ZipfDistribution;

import java.util.*;

public class VaryQueries {
    private static final StreamID sid = new StreamID(0);
    private static Random rand = new Random();
    private static Runtime runtime = Runtime.getRuntime();
    private static final int N = 1000000;

    public static void main(String[] args) {
        String tdLoc = "/tmp/tdstore", eLoc = "/tmp/estore";
        try {
            runtime.exec(new String[]{"rm", "-rf", tdLoc, eLoc}).waitFor();
            TimeDecayedStore tdStore = new TimeDecayedStore(tdLoc, new WBMHBucketMerger(2));
            //EnumeratedStore eStore = new EnumeratedStore(eLoc);

            tdStore.registerStream(sid);
            //eStore.registerStream(sid);
            for (int n = 0; n < N; ++n) {
                int v = 1 + rand.nextInt(100);
                Collection<FlaggedValue> fv = Collections.singletonList(new FlaggedValue(v));
                tdStore.append(sid, fv);
                //eStore.append(sid, fv);
            }

            Statistics countInflation = new Statistics(true);

            System.out.println("#s\tCount inflation");
            double[] svals = {1, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2, 2.25, 2.5, 2.75, 3, 3.5, 4, 4.5, 5, 6, 7, 8, 9, 10};
            for (double s: svals) {
                ZipfDistribution zipf = new ZipfDistribution(N, s);
                for (int q = 0; q < 100000; ++q) {
                    int v0 = N - zipf.sample(), v1 = N - zipf.sample();
                    int l = Math.min(v0, v1), r = Math.max(v0, v1);
                    double tdCount = (Integer)tdStore.query(sid, Bucket.QUERY_COUNT, l, r);
                    //double eCount = (Integer)eStore.query(sid, Bucket.QUERY_COUNT, l, r);
                    double eCount = r - l + 1;
                    countInflation.addObservation(tdCount / eCount);
                }

                System.out.println(s + "\t" + countInflation.getErrorbars());
            }

            tdStore.close();
            //eStore.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
