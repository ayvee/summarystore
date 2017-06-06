package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.Aggregates.SimpleCountOperator;
import com.samsung.sra.DataStore.CountBasedWBMH;
import com.samsung.sra.DataStore.RationalPowerWindowing;
import com.samsung.sra.DataStore.SummaryStore;

import java.util.concurrent.ThreadLocalRandom;

public class MeasureThroughput {
    private static final String loc_prefix = "/tmp/tdstore_";

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("SYNTAX: MeasureThroughput numValuesPerThread numThreads");
            System.exit(2);
        }
        long T = Long.parseLong(args[0].replace("_", ""));
        int nThreads = Integer.parseInt(args[1]);
        Runtime.getRuntime().exec(new String[]{"sh", "-c", "rm -rf " + loc_prefix + "*"}).waitFor();

        try (SummaryStore store = new SummaryStore(null/*loc_prefix + "throughput"*/)) {
            StreamWriter[] writers = new StreamWriter[nThreads];
            Thread[] writerThreads = new Thread[nThreads];
            for (int i = 0; i < nThreads; ++i) {
                writers[i] = new StreamWriter(store, i, T);
                writerThreads[i] = new Thread(writers[i]);
            }
            long w0 = System.currentTimeMillis();
            for (int i = 0; i < nThreads; ++i) {
                writerThreads[i].start();
            }
            for (int i = 0; i < nThreads; ++i) {
                writerThreads[i].join();
            }
            long we = System.currentTimeMillis();
            System.out.printf("Write throughput = %,.0f appends/s\n",  (nThreads * T * 1000d / (we - w0)));
            for (int i = 0; i < nThreads; ++i) {
                store.flush(i);
            }
            //store.printWindowState(streamID);

            long f0 = System.currentTimeMillis();
            store.query(0, 0, T - 1, 0);
            long fe = System.currentTimeMillis();
            System.out.println("Time to run longest query, spanning [0, T) = " + ((fe - f0) / 1000d) + " sec");
        }
    }

    private static class StreamWriter implements Runnable {
        private final long streamID, N;
        private final SummaryStore store;
        private final ThreadLocalRandom random;

        private StreamWriter(SummaryStore store, long streamID, long N) throws Exception {
            this.store = store;
            this.streamID = streamID;
            this.N = N;
            this.random = ThreadLocalRandom.current();
            store.registerStream(streamID, false,
                    new CountBasedWBMH(new RationalPowerWindowing(1, 1, 6, 1), 2_000_000),
                    new SimpleCountOperator());
        }

        @Override
        public void run() {
            try {
                for (long t = 0; t < N; ++t) {
                    long v = random.nextLong(100);
                    store.append(streamID, t, v);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
