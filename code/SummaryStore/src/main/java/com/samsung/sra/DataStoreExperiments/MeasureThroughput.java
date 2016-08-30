package com.samsung.sra.DataStoreExperiments;

import com.moandjiezana.toml.Toml;
import com.samsung.sra.DataStore.Aggregates.SimpleCountOperator;
import com.samsung.sra.DataStore.CountBasedWBMH;
import com.samsung.sra.DataStore.RationalPowerWindowing;
import com.samsung.sra.DataStore.SummaryStore;

public class MeasureThroughput {
    private static final String loc_prefix = "/tmp/tdstore_";
    private static final long streamID = 0;
    private static final long T = 100_000_000;
    private static final String streamConf =
              "interarrivals = {distribution = \"Fixed\", value = 1}\n"
            + "values = {distribution = \"Uniform\", min = 0, max = 100}\n"
            + "random-seed = 0";


    public static void main(String[] args) throws Exception {
        Runtime.getRuntime().exec(new String[]{"sh", "-c", "rm -rf " + loc_prefix + "*"}).waitFor();

        try (SummaryStore store = new SummaryStore(loc_prefix + "throughput")) {
            store.registerStream(streamID,
                    new CountBasedWBMH(new RationalPowerWindowing(1, 1, 6, 1), 2_000_000),
                    new SimpleCountOperator());

            StreamGenerator generator = new RandomStreamGenerator(new Toml().read(streamConf));
            long w0 = System.currentTimeMillis();
            generator.generate(T, (t, v) -> {
                try {
                    store.append(streamID, t, v);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            store.flush(streamID);
            long we = System.currentTimeMillis();
            System.out.println("Write throughput = " + (T * 1000d / (we - w0)) + " appends/s");
            store.printBucketState(streamID);

            long f0 = System.currentTimeMillis();
            store.query(streamID, 0, T - 1, 0);
            long fe = System.currentTimeMillis();
            System.out.println("Time to run longest query, spanning [0, T) = " + ((fe - f0) / 1000d) + " sec");
        }
    }
}
