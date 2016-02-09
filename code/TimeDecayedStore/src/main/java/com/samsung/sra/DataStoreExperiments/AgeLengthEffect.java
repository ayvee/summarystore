package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.*;

import java.util.*;

public class AgeLengthEffect {
    private static final StreamID sid = new StreamID(0);
    private static Random rand = new Random();
    private static Runtime runtime = Runtime.getRuntime();
    private static final int N = 1000000;
    private static String prefix = "/tmp/tds_";

    /**
     * Uniform random integer in [l, r]
     */
    private static int urand(int l, int r) {
        assert r >= l;
        if (r == l) {
            return l;
        } else {
            return l + rand.nextInt(r - l + 1);
        }
    }

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

            ArrayList<Integer> age_ls = new ArrayList<Integer>(), age_rs = new ArrayList<Integer>();
            for (int age_l = 1; age_l < N / 10; age_l *= 10) {
                age_ls.add(age_l);
                age_rs.add(Math.min(age_l * 10 - 1, N));
            }
            for (int age_l = N / 10; age_l <= N; age_l += N / 10) {
                int age_r = age_l + N / 10 - 1;
                age_ls.add(age_l);
                age_rs.add(Math.min(age_r, N));
            }
            ArrayList<Integer> length_ls = new ArrayList<Integer>(), length_rs = new ArrayList<Integer>();
            for (int length_l = 1; length_l < N / 10; length_l *= 10) {
                length_ls.add(length_l);
                length_rs.add(Math.min(length_l * 10 - 1, N));
            }
            for (int length_l = N / 10; length_l <= N; length_l += N / 10) {
                int length_r = length_l + N / 10 - 1;
                length_ls.add(length_l);
                length_rs.add(Math.min(length_r, N));
            }
            System.out.print("age/length");
            for (int j = 0; j < length_ls.size(); ++j) {
                //System.out.print("\t" + length_ls.get(j) + "-" + length_rs.get(j));
                System.out.print("\t" + length_ls.get(j));
            }
            System.out.println();

            for (int i = 0; i < age_ls.size(); ++i) {
                int age_l = age_ls.get(i), age_r = age_rs.get(i);
                //System.out.print(age_l + "-" + age_r);
                System.out.print(age_l);
                for (int j = 0; j < length_ls.size(); ++j) {
                    int length_l = length_ls.get(j), length_r = length_rs.get(j);
                    Statistics stats = new Statistics(false);
                    for (int q = 0; q < 10000; ++q) {
                        int age = urand(age_l, age_r);
                        if (age + length_l - 1 > N)
                            continue;
                        int r = N - age;
                        int l_min = Math.max(N - (age + length_r - 1), 0), l_max = N - (age + length_l - 1);
                        int l = urand(l_min, l_max);
                        int scount = (Integer)datastores.get("summarystore").query(sid, Bucket.QUERY_COUNT, l, r);
                        int lcount = (Integer)datastores.get("linearstore").query(sid, Bucket.QUERY_COUNT, l, r);
                        //int ecount = r - l + 1;
                        stats.addObservation(scount <= lcount ? 1 : 0);
                        if (scount <= lcount) System.err.println(age + "\t" + (r - l + 1));
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
