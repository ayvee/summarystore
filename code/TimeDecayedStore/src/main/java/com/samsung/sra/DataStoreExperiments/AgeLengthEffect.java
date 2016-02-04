package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.*;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;

import java.util.*;

public class AgeLengthEffect {
    private static final StreamID sid = new StreamID(0);
    private static Random rand = new Random();
    private static Runtime runtime = Runtime.getRuntime();
    private static final int N = 1000000;
    //private static String tdLoc = "/tmp/tdstore", eLoc = "/tmp/estore";
    private static String prefix = "/tmp/tds_";

    public static void main(String[] args) {

        try {
            runtime.exec(new String[]{"sh", "-c", "rm -rf " + prefix + "*"}).waitFor();
            LinkedHashMap<String, DataStore> datastores = new LinkedHashMap<String, DataStore>();
            datastores.put("enumeration", new EnumeratedStore(prefix + "enumeration"));
            datastores.put("summarystore", new TimeDecayedStore(prefix + "summarystore", new WBMHBucketMerger(2)));
            datastores.put("linearstore", new TimeDecayedStore(prefix + "linearstore", new FixedSizeBucketMerger(4546)));

            for (DataStore ds: datastores.values()) {
                ds.registerStream(sid);
            }
            for (int n = 0; n < N; ++n) {
                int v = 1 + rand.nextInt(100);
                Collection<FlaggedValue> fv = Collections.singletonList(new FlaggedValue(v));
                for (DataStore ds: datastores.values()) {
                    ds.append(sid, fv);
                }
            }

            /*Map<String, Long> storeSizes = new HashMap<String, Long>();
            for (Map.Entry<String, DataStore> entry: datastores.entrySet()) {
                String name = entry.getKey();
                DataStore ds = entry.getValue();
                long size = ds.getStoreSizeInBytes();
                storeSizes.put(name, size);
            }*/

            ArrayList<Integer> ages = new ArrayList<Integer>();
            for (int age = 1; age <= N / 10; age *= 10) {
                ages.add(age);
            }
            for (int age = N / 10; age <= N; age += N / 10) {
                ages.add(age);
            }
            ArrayList<Integer> length_ls = new ArrayList<Integer>(), length_rs = new ArrayList<Integer>();
            for (int length_r = 1; length_r <= N / 10; length_r *= 10) {
                int length_l = length_r / 10 + 1;
                length_ls.add(length_l);
                length_rs.add(length_r);
            }
            for (int length_l = N / 10 + 1; length_l <= N; length_l += N / 10) {
                int length_r = length_l + N / 10 - 1;
                length_ls.add(length_l);
                length_rs.add(length_r);
            }
            System.out.print("age/length");
            for (int j = 0; j < length_ls.size(); ++j) {
                //System.out.print("\t" + length_ls.get(j) + "-" + length_rs.get(j));
                System.out.print("\t" + length_rs.get(j));
            }
            System.out.println();

            for (int age: ages) {
                System.out.print(age);
                for (int j = 0; j < length_ls.size(); ++j) {
                    int length_l = length_ls.get(j), length_r = length_rs.get(j);
                    Statistics stats = new Statistics(false);
                    if (age + length_l > N) {
                        System.out.print("\tNaN");
                        continue;
                    }
                    int r = (N - 1) - age; // age 0 = newest element, age N-1 = newest element
                    int l_min = Math.max(N - (age + length_r), 0), l_max = N - (age + length_l);
                    UniformIntegerDistribution uniform = new UniformIntegerDistribution(l_min, l_max);
                    for (int q = 0; q < 1000; ++q) {
                        int l = uniform.sample();
                        int scount = (Integer)datastores.get("summarystore").query(sid, Bucket.QUERY_COUNT, l, r);
                        int lcount = (Integer)datastores.get("linearstore").query(sid, Bucket.QUERY_COUNT, l, r);
                        //int ecount = r - l + 1;
                        stats.addObservation(scount <= lcount ? 1 : 0);
                    }
                    System.out.print("\t" + stats.getAverage());
                }
                System.out.println();
            }

            for (DataStore ds: datastores.values()) {
                ds.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
