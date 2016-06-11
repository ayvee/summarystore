package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.*;
import org.rocksdb.RocksDBException;

import java.util.LinkedHashMap;
import java.util.Map;

public class MeasureThroughput {
    private static String loc_prefix = "/tmp/tdstore_";
    private static long streamID = 0;

    public static void main(String[] args) throws Exception {
        Runtime.getRuntime().exec(new String[]{"rm", "-rf", loc_prefix + "*"}).waitFor();
        long T = 500_000_000;
        //long storageSavingsFactor = 1;
        //long W = T / 2 / storageSavingsFactor; // # windows
        long W = T;

        long Q = 1000;

        InterarrivalDistribution interarrivals = new FixedInterarrival(1);
        ValueDistribution values = new UniformValues(0, 100);

        LinkedHashMap<String, SummaryStore> stores = new LinkedHashMap<>();
        System.out.println("Testing a store with " + T + " elements and constant size 1 bucketing (0% storage savings)");
        registerStore(stores, "linearstore", new CountBasedWBMH(new RationalPowerWindowing(1, 0)));

        StreamGenerator generator = new StreamGenerator(interarrivals, values, 0);
        long w0 = System.currentTimeMillis();
        generator.generate(T, (t, v) -> {
            try {
                for (SummaryStore store: stores.values()) {
                    store.append(streamID, t, v);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        long we = System.currentTimeMillis();
        System.out.println("Write throughput = " + (T * 1000d / (we - w0)) + " appends/s");

        SummaryStore store = stores.get("linearstore");

        long f0 = System.currentTimeMillis();
        store.query(streamID, 0, T-1, 0, null);
        long fe = System.currentTimeMillis();
        System.out.println("Time to run longest query, spanning [0, T) = " + ((fe - f0) / 1000d) + " sec");
    }

    private static void registerStore(Map<String, SummaryStore> stores, String storeName, WindowingMechanism windowingMechanism) throws RocksDBException, StreamException {
        SummaryStore store = new SummaryStore(loc_prefix + storeName);
        //SummaryStore store = new SummaryStore(new MainMemoryBucketStore());
        store.registerStream(streamID, windowingMechanism);
        stores.put(storeName, store);
    }
}
