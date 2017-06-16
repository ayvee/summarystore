package com.samsung.sra.DataStore.Ingest;

import com.samsung.sra.DataStore.Storage.BackingStoreException;
import com.samsung.sra.DataStore.Storage.StreamWindowManager;
import com.samsung.sra.DataStore.SummaryWindow;
import com.samsung.sra.DataStore.Utilities;
import com.samsung.sra.DataStore.Windowing;
import com.samsung.sra.DataStore.WindowingMechanism;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

/**
 * Buffered mode setup: a pipeline of Ingester -> Summarizer -> Writer, as well as a Merger running in parallel.
 * We manage three internal threads for Summarizer, Writer and Merger, and the Ingester code runs in the external
 * user thread(s) calling SummaryStore.append().
 *
 * Unbuffered mode setup: same except no Ingester or Summarizer. External user threads run appendUnbuffered() instead of
 * Ingester
 */
public class CountBasedWBMH implements WindowingMechanism {
    private static Logger logger = LoggerFactory.getLogger(CountBasedWBMH.class);
    /** Used to throttle Writer and Merger input queues */
    private static final int MAX_QUEUE_SIZE = 10_000;

    private transient StreamWindowManager windowManager;

    private final long sizeOfNewestWindow;
    private final long bufferSize;

    private final Ingester ingester;
    private final Summarizer summarizer;
    private final Writer writer;
    private final Merger merger;
    private final FlushBarrier flushBarrier;

    private final BlockingQueue<IngestBuffer> emptyBuffers = new LinkedBlockingQueue<>();
    private final BlockingQueue<IngestBuffer> partialBuffers = new LinkedBlockingQueue<>();
    private final BlockingQueue<IngestBuffer> summarizerQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<SummaryWindow> writerQueue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
    private final BlockingQueue<Merger.WindowInfo> mergerQueue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);

    private long N = 0;

    /**
     * Buffers up to totalBufferSize elements in memory, split across numBuffers buffers of equal size, deferring value
     * writes until either buffers fill up or flush is called.
     */
    public CountBasedWBMH(Windowing windowing, int totalBufferSize, int numBuffers, int windowsPerMergeBatch) {
        assert numBuffers > 0;
        this.sizeOfNewestWindow = windowing.getSizeOfFirstWindow();
        int[] bufferWindowLengths = windowing.getWindowsCoveringUpto(totalBufferSize / numBuffers)
                .stream().mapToInt(Long::intValue).toArray();
        bufferSize = IntStream.of(bufferWindowLengths).sum(); // actual buffer size, <= numValuesToBuffer
        logger.info("{} ingest buffers each covering {} windows and {} values", numBuffers, bufferWindowLengths.length, bufferSize);
        if (bufferSize == 0 && sizeOfNewestWindow > 1) {
            throw new UnsupportedOperationException("do not yet support unbuffered ingest when size of newest window > 1");
        }

        flushBarrier = new FlushBarrier();
        if (bufferSize > 0) {
            for (int i = 0; i < numBuffers; ++i) {
                emptyBuffers.add(new IngestBuffer((int) bufferSize));
            }
        }
        ingester = new Ingester(emptyBuffers, summarizerQueue);
        summarizer = new Summarizer(bufferWindowLengths, emptyBuffers, partialBuffers, summarizerQueue, writerQueue, flushBarrier);
        writer = new Writer(writerQueue, mergerQueue, flushBarrier);
        //merger = new HeapMerger(windowing, mergerQueue, flushBarrier);
        merger = new BatchingHeapMerger(windowing, mergerQueue, flushBarrier, windowsPerMergeBatch);
    }

    public CountBasedWBMH(Windowing windowing, int totalBufferSize) {
        this(windowing, totalBufferSize, 2, 1);
    }

    public CountBasedWBMH(Windowing windowing) {
        this(windowing, 0);
    }

    @Override
    public void populateTransientFields(StreamWindowManager windowManager) {
        this.windowManager = windowManager;
        if (bufferSize > 0) {
            summarizer.populateTransientFields(windowManager);
        }
        writer.populateTransientFields(windowManager);
        merger.populateTransientFields(windowManager);

        if (bufferSize > 0) {
            new Thread(summarizer, windowManager.streamID + "-summarizer").start();
        }
        new Thread(writer, windowManager.streamID + "-writer").start();
        new Thread(merger, windowManager.streamID + "-merger").start();
    }

    @Override
    public void append(long ts, Object value) throws BackingStoreException {
        if (bufferSize > 0) {
            if (N % 1_000_000 == 0) {
                logger.info("N = {}M, queue lengths: emptyBuffers = {}, summarizer = {}, writer = {}, merger = {}",
                        N / 1_000_000, emptyBuffers.size(), summarizerQueue.size(), writerQueue.size(), mergerQueue.size());
            }
            ingester.append(ts, value);
        } else {
            appendUnbuffered(ts, value);
        }
        ++N;
    }

    private void appendUnbuffered(long timestamp, Object value) throws BackingStoreException {
        /*// insert newest element, creating a new window for it if necessary
        if (sizeOfNewestWindow > 1 && lastSWID != -1) {
            SummaryWindow lastWindow = windowManager.getSummaryWindow(lastSWID);
            if (lastWindow.ce - lastWindow.cs + 1 < sizeOfNewestWindow) {
                // newest window isn't yet full; insert value into it
                lastWindow.ce = N;
                lastWindow.te = timestamp;
                windowManager.insertIntoSummaryWindow(lastWindow, timestamp, value);
                windowManager.putSummaryWindow(lastWindow);
                return;
            }
        }*/
        assert sizeOfNewestWindow == 1;
        SummaryWindow newWindow = windowManager.createEmptySummaryWindow(timestamp, timestamp, N, N);
        windowManager.insertIntoSummaryWindow(newWindow, timestamp, value);
        windowManager.putSummaryWindow(newWindow);
        Utilities.put(writerQueue, newWindow);
        //Utilities.put(mergerQueue, new Merger.WindowInfo(timestamp, 1L));
    }

    private void flush(boolean shutdown) throws BackingStoreException {
        long threshold = flushBarrier.getNextFlushThreshold();
        if (bufferSize > 0) {
            ingester.flush(shutdown);
            flushBarrier.wait(FlushBarrier.SUMMARIZER, threshold);
            IngestBuffer partialBuffer = partialBuffers.poll();
            if (partialBuffer != null) {
                N -= partialBuffer.size(); // need to undo since we pulled them out of the pipeline
                for (int i = 0; i < partialBuffer.size(); ++i) {
                    appendUnbuffered(partialBuffer.getTimestamp(i), partialBuffer.getValue(i));
                    ++N;
                }
                partialBuffer.clear();
                Utilities.put(emptyBuffers, partialBuffer);
            }
            assert partialBuffers.isEmpty();
        }
        Utilities.put(writerQueue, shutdown ? Writer.SHUTDOWN_SENTINEL : Writer.FLUSH_SENTINEL);
        flushBarrier.wait(FlushBarrier.WRITER, threshold);
        Utilities.put(mergerQueue, shutdown ? Merger.SHUTDOWN_SENTINEL : Merger.FLUSH_SENTINEL);
        flushBarrier.wait(FlushBarrier.MERGER, threshold);
    }

    @Override
    public void flush() throws BackingStoreException {
        flush(false);
    }

    @Override
    public void close() throws BackingStoreException {
        flush(true);
    }

    static class FlushBarrier implements Serializable {
        static final int SUMMARIZER = 0, WRITER = 1, MERGER = 2;

        private final AtomicLong flushCount = new AtomicLong(0);
        private final Serializable monitor = new Object[0]; // any serializable object would do
        private long[] counters = new long[MERGER + 1];

        private long getNextFlushThreshold() {
            return flushCount.incrementAndGet();
        }

        private void wait(int type, long threshold) {
            synchronized (monitor) {
                while (counters[type] < threshold) {
                    try {
                        monitor.wait();
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }

        void notify(int type) {
            synchronized (monitor) {
                ++counters[type];
                monitor.notifyAll();
            }
        }
    }
}
