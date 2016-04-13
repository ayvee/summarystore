package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.*;
import org.rocksdb.RocksDBException;

import java.util.LinkedHashMap;
import java.util.Map;

class CompareWindowingSchemes {
    private static String loc_prefix = "/tmp/tdstore_";
    private static final StreamID streamID = new StreamID(0);

    public static void main(String[] args) throws Exception {
        long T = 100_000_000;
        int W = 5_000_000;

        Runtime.getRuntime().exec(new String[]{"sh", "-c", "rm -rf " + loc_prefix + "*"}).waitFor();
        LinkedHashMap<String, SummaryStore> stores = new LinkedHashMap<>();
        registerStore(stores, "expoential", new CountBasedWBMH(ExponentialWindowLengths.getWindowingOfSize(T, W)));
        //registerStore(stores, "expoential-slow", new SlowCountBasedWBMH(ExponentialWindowLengths.getWindowingOfSize(T, W)));
        //registerStore(stores, "constant", new CountBasedWBMH(PolynomialWindowLengths.getWindowingOfSize(0, T, W)));
        //registerStore(stores, "linear", new CountBasedWBMH(PolynomialWindowLengths.getWindowingOfSize(1, T, W)));
        InterarrivalDistribution interarrivals = new FixedInterarrival(1);
        ValueDistribution values = new UniformValues(0, 100);
        WriteLoadGenerator generator = new WriteLoadGenerator(interarrivals, values, streamID, stores.values());
        generator.generateUntil(T);

        for (Map.Entry<String, SummaryStore> entry : stores.entrySet()) {
            Timestamp l = new Timestamp(5), r = new Timestamp(10);
            System.out.println(entry.getKey() + ": count[" + l + ", " + r + "] = " +
                    entry.getValue().query(streamID, l, r, QueryType.COUNT, null));
        }

        // FIXME: close() stores when done
    }

    private static void registerStore(Map<String, SummaryStore> stores, String name, WindowingMechanism windowingMechanism) throws RocksDBException, StreamException {
        //SummaryStore store = new SummaryStore(new RocksDBBucketStore(loc_prefix + name));
        SummaryStore store = new SummaryStore(new MainMemoryBucketStore());
        store.registerStream(streamID, windowingMechanism);
        stores.put(name, store);
    }
}
