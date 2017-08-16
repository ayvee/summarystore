package com.samsung.sra.experiments;

import com.samsung.sra.datastore.RationalPowerWindowing;
import com.samsung.sra.datastore.SummaryStore;
import com.samsung.sra.datastore.aggregates.CMSOperator;
import com.samsung.sra.datastore.aggregates.SimpleCountOperator;
import com.samsung.sra.datastore.ingest.CountBasedWBMH;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;

public class MeasureThroughput {
    private static final String directory = "/mnt/md0/tdstore_throughput";
    private static final Logger logger = LoggerFactory.getLogger(MeasureThroughput.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("SYNTAX: MeasureThroughput numValuesPerThread numThreads [numParallelThreads]");
            System.exit(2);
        }
        long T = Long.parseLong(args[0].replace("_", ""));
        int nThreads = Integer.parseInt(args[1]);
        Semaphore parallelismSem = args.length > 2
                ? new Semaphore(Integer.parseInt(args[2]))
                : null;
        Runtime.getRuntime().exec(new String[]{"sh", "-c", "rm -rf " + directory}).waitFor();

        try (SummaryStore store = new SummaryStore(directory, new SummaryStore.Options().setKeepReadIndexes(false))) {
            StreamWriter[] writers = new StreamWriter[nThreads];
            Thread[] writerThreads = new Thread[nThreads];
            for (int i = 0; i < nThreads; ++i) {
                writers[i] = new StreamWriter(store, parallelismSem, i, T);
                writerThreads[i] = new Thread(writers[i], i + "-appender");
            }
            long w0 = System.currentTimeMillis();
            for (int i = 0; i < nThreads; ++i) {
                writerThreads[i].start();
            }
            for (int i = 0; i < nThreads; ++i) {
                writerThreads[i].join();
            }
            long we = System.currentTimeMillis();
            logger.info("Write throughput = {} appends/s",  String.format("%,.0f", (nThreads * T * 1000d / (we - w0))));
            logger.info("Stream 0 has {} elements in {} windows", T, store.getNumSummaryWindows(0L));

            /*long f0 = System.currentTimeMillis();
            store.query(0, 0, T - 1, 0);
            long fe = System.currentTimeMillis();
            logger.info("Time to run longest query, spanning [0, T) = {} sec", (fe - f0) / 1000d);*/
        }
    }

    private static class StreamWriter implements Runnable {
        private final long streamID, N;
        private final SummaryStore store;
        private final Semaphore semaphore;
        private final CountBasedWBMH wbmh;
        private final ThreadLocalRandom random;

        private StreamWriter(SummaryStore store, Semaphore semaphore, long streamID, long N) throws Exception {
            this.store = store;
            this.semaphore = semaphore;
            this.streamID = streamID;
            this.N = N;
            this.random = ThreadLocalRandom.current();
            this.wbmh = new CountBasedWBMH(new RationalPowerWindowing(1, 1, 23, 1))
                    .setValuesAreLongs(true)
                    .setBufferSize(1_600_000_000)
                    .setWindowsPerMergeBatch(1_000_000_000)
                    .setParallelizeMerge(10);
            store.registerStream(streamID, false, wbmh,
                    new SimpleCountOperator(), new CMSOperator(5, 1000, 0));
        }

        @Override
        public void run() {
            if (semaphore != null) semaphore.acquireUninterruptibly();
            try {
                for (long t = 0; t < N; ++t) {
                    long v = random.nextLong(100);
                    store.append(streamID, t, v);
                }
                /*store.flush(streamID);
                wbmh.setBufferSize(0);*/
                wbmh.flushAndSetUnbuffered();
                logger.info("Populated stream {}", streamID);
                if (semaphore != null) {
                    store.unloadStream(streamID);
                    logger.info("Unloaded stream {}", streamID);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                if (semaphore != null) semaphore.release();
            }
        }
    }
}
