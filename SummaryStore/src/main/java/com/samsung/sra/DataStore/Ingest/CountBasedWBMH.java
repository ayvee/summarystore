package com.samsung.sra.DataStore.Ingest;

import com.samsung.sra.DataStore.Storage.BackingStoreException;
import com.samsung.sra.DataStore.Storage.StreamWindowManager;
import com.samsung.sra.DataStore.SummaryWindow;
import com.samsung.sra.DataStore.Utilities;
import com.samsung.sra.DataStore.Windowing;
import com.samsung.sra.DataStore.WindowingMechanism;
import org.apache.commons.math3.util.Pair;
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
 * Unbuffered mode setup: one internal thread, running a Merger; and external user thread(s) running appendUnbuffered()
 * code.
 */
public class CountBasedWBMH implements WindowingMechanism {
    private static Logger logger = LoggerFactory.getLogger(CountBasedWBMH.class);

    private transient StreamWindowManager windowManager;

    private final long sizeOfNewestWindow;
    private final long bufferSize;

    private final Ingester ingester;
    private final Summarizer summarizer;
    private final Writer writer;
    private final Merger merger;

    private final BlockingQueue<Merger.WindowInfo> newWindowNotifications; // input queue for merger
    private final FlushHandler flushHandler;

    private long N = 0;

    /**
     * Buffers up to totalBufferSize elements in memory, split across numBuffers buffers of equal size, deferring value
     * writes until either buffers fill up or flush is called.
     */
    public CountBasedWBMH(Windowing windowing, int totalBufferSize, int numBuffers) {
        assert numBuffers > 0;
        this.sizeOfNewestWindow = windowing.getSizeOfFirstWindow();
        int[] bufferWindowLengths = windowing.getWindowsCoveringUpto(totalBufferSize / numBuffers)
                .stream().mapToInt(Long::intValue).toArray();
        bufferSize = IntStream.of(bufferWindowLengths).sum(); // actual buffer size, <= numValuesToBuffer
        logger.info("Ingest buffer covers {} windows and {} values", bufferWindowLengths.length, bufferSize);
        if (bufferSize == 0 && sizeOfNewestWindow > 1) {
            throw new UnsupportedOperationException("do not yet support unbuffered ingest when size of newest window > 1");
        }

        newWindowNotifications = new LinkedBlockingQueue<>();
        flushHandler = new FlushHandler();

        if (bufferSize > 0) {
            BlockingQueue<IngestBuffer> emptyBuffers = new LinkedBlockingQueue<>();
            BlockingQueue<IngestBuffer> buffersToSummarize = new LinkedBlockingQueue<>();
            BlockingQueue<SummaryWindow> windowsToWrite = new LinkedBlockingQueue<>();
            for (int i = 0; i < 10; ++i) {
                emptyBuffers.add(new IngestBuffer((int) bufferSize));
            }

            ingester = new Ingester(emptyBuffers, buffersToSummarize);
            summarizer = new Summarizer(bufferWindowLengths, emptyBuffers, buffersToSummarize, windowsToWrite, flushHandler);
            writer = new Writer(windowsToWrite, newWindowNotifications, flushHandler);
        } else {
            ingester = null;
            summarizer = null;
            writer = null;
        }
        merger = new HeapMerger(windowing, newWindowNotifications, flushHandler);
    }

    public CountBasedWBMH(Windowing windowing, int totalBufferSize) {
        this(windowing, totalBufferSize, 2);
    }

    public CountBasedWBMH(Windowing windowing) {
        this(windowing, 0);
    }

    @Override
    public void populateTransientFields(StreamWindowManager windowManager) {
        this.windowManager = windowManager;
        if (bufferSize > 0) {
            summarizer.populateTransientFields(windowManager);
            writer.populateTransientFields(windowManager);
        }
        merger.populateTransientFields(windowManager);

        if (bufferSize > 0) {
            new Thread(summarizer).start();
            new Thread(writer).start();
        }
        new Thread(merger).start();
    }

    @Override
    public void append(long ts, Object value) throws BackingStoreException {
        if (bufferSize > 0) {
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
        Utilities.offerAndConfirm(newWindowNotifications, new Merger.WindowInfo(timestamp, 1L));
    }

    private void flush(boolean shutdown) throws BackingStoreException {
        long threshold = flushHandler.getNextFlushThreshold();
        if (bufferSize > 0) {
            Pair<IngestBuffer, BlockingQueue<IngestBuffer>> toFlush = ingester.flush(shutdown);
            IngestBuffer partlyFullBuffer = toFlush.getFirst();
            BlockingQueue<IngestBuffer> emptyBuffers = toFlush.getSecond();
            flushHandler.waitForIngestCompletion(threshold);
            if (partlyFullBuffer != null) {
                N -= partlyFullBuffer.size(); // undo the pretend-insert
                for (int i = 0; i < partlyFullBuffer.size(); ++i) {
                    appendUnbuffered(partlyFullBuffer.getTimestamp(i), partlyFullBuffer.getValue(i));
                    ++N;
                }
                partlyFullBuffer.clear();
                Utilities.offerAndConfirm(emptyBuffers, partlyFullBuffer);
            }
        }
        Utilities.offerAndConfirm(newWindowNotifications, shutdown ? HeapMerger.SHUTDOWN_SENTINEL : HeapMerger.FLUSH_SENTINEL);
        flushHandler.waitForMergeCompletion(threshold);
    }

    @Override
    public void flush() throws BackingStoreException {
        flush(false);
    }

    @Override
    public void close() throws BackingStoreException {
        flush(true);
    }

    static class FlushHandler implements Serializable {
        private final AtomicLong flushCount = new AtomicLong(0);
        private final Serializable monitor = new Object[0]; // any serializable object would do
        private long numSummarizerFlushes = 0, numWriterFlushes = 0, numMergerFlushes = 0;

        private long getNextFlushThreshold() {
            return flushCount.incrementAndGet();
        }

        private void waitForIngestCompletion(long threshold) {
            synchronized (monitor) {
                while (numSummarizerFlushes < threshold || numWriterFlushes < threshold) {
                    try {
                        monitor.wait();
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }

        private void waitForMergeCompletion(long threshold) {
            synchronized (monitor) {
                while (numMergerFlushes < threshold) {
                    try {
                        monitor.wait();
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }

        void notifySummarizerFlushed() {
            synchronized (monitor) {
                ++numSummarizerFlushes;
                monitor.notifyAll();
            }
        }

        void notifyWriterFlushed() {
            synchronized (monitor) {
                ++numWriterFlushes;
                monitor.notifyAll();
            }
        }

        void notifyMergerFlushed() {
            synchronized (monitor) {
                ++numMergerFlushes;
                monitor.notifyAll();
            }
        }
    }

}
