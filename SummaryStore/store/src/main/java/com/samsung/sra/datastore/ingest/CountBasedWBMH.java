/*
* Copyright 2016 Samsung Research America. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.samsung.sra.datastore.ingest;

import com.samsung.sra.datastore.storage.BackingStoreException;
import com.samsung.sra.datastore.storage.StreamWindowManager;
import com.samsung.sra.datastore.SummaryWindow;
import com.samsung.sra.datastore.Utilities;
import com.samsung.sra.datastore.Windowing;
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
public class CountBasedWBMH implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(CountBasedWBMH.class);
    /** Used to throttle Writer and Merger input queues */
    private static final int MAX_QUEUE_SIZE = 10_000;

    private transient StreamWindowManager windowManager;

    private final Windowing windowing;
    //private final long sizeOfNewestWindow;

    private long bufferSize;
    private boolean valuesAreLongs;

    private final Ingester ingester;
    private final Summarizer summarizer;
    private final Writer writer;
    private final HeapMerger merger;
    private final FlushBarrier flushBarrier;

    private final BlockingQueue<IngestBuffer> emptyBuffers = new LinkedBlockingQueue<>();
    private final BlockingQueue<IngestBuffer> partialBuffers = new LinkedBlockingQueue<>();
    private final BlockingQueue<IngestBuffer> summarizerQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<SummaryWindow> writerQueue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
    private final BlockingQueue<Merger.WindowInfo> mergerQueue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);

    private long N = 0;

    public CountBasedWBMH(Windowing windowing) {
        this.windowing = windowing;
        //this.sizeOfNewestWindow = windowing.getSizeOfFirstWindow();

        bufferSize = 0;
        valuesAreLongs = false;
        flushBarrier = new FlushBarrier();
        ingester = new Ingester(emptyBuffers, summarizerQueue);
        summarizer = new Summarizer(null, emptyBuffers, partialBuffers, summarizerQueue, writerQueue, flushBarrier);
        writer = new Writer(writerQueue, mergerQueue, flushBarrier);
        //merger = new UnbatchedHeapMerger(windowing, mergerQueue, flushBarrier);
        merger = new HeapMerger(windowing, mergerQueue, flushBarrier, 1);
    }

    /**
     * Use numBuffers buffers of size up to totalBufferSize / numBuffers each. Actual buffer size may be smaller since
     * buffers need to be aligned to window boundaries.
     *
     * WARNING: please ensure stream has been flushed before calling */
    public CountBasedWBMH setBufferSize(int totalBufferSize, int numBuffers) {
        destroyEmptyBuffers();
        int[] bufferWindowLengths = windowing.getWindowsCoveringUpto(totalBufferSize / numBuffers)
                .stream().mapToInt(Long::intValue).toArray();
        summarizer.setWindowLengths(bufferWindowLengths);
        bufferSize = IntStream.of(bufferWindowLengths).sum(); // actual buffer size, <= numValuesToBuffer
        logger.info("{} ingest buffers each covering {} windows and {} values", numBuffers, bufferWindowLengths.length, bufferSize);
        /*if (bufferSize == 0 && sizeOfNewestWindow > 1) {
            throw new UnsupportedOperationException("do not yet support unbuffered ingest when size of newest window > 1");
        }*/
        if (bufferSize > 0) {
            assert numBuffers > 0;
            for (int i = 0; i < numBuffers; ++i) {
                emptyBuffers.add(valuesAreLongs
                ? new LongIngestBuffer((int) bufferSize)
                : new ObjectIngestBuffer((int) bufferSize));
            }
        }
        return this;
    }

    /**
     * Use 2 buffers of size up to totalBufferSize / 2 each. Actual buffer size may be smaller since buffers need to be
     * aligned to window boundaries.
     *
     * WARNING: please ensure stream has been flushed before calling */
    public CountBasedWBMH setBufferSize(int totalBufferSize) {
        return this.setBufferSize(totalBufferSize, 2);
    }

    /**
     * If all values are longs and this flag is set, enables a special code path using off-heap long[] value arrays in
     * the ingest buffer.
     *
     * Only takes effect on the next setBufferSize() call (be careful about method call order, esp. when constructing).
     */
    public CountBasedWBMH setValuesAreLongs(boolean valuesAreLongs) {
        this.valuesAreLongs = valuesAreLongs;
        return this;
    }

    /**
     * Only call WBMH merge once every W window appends. Batches merges, merging chains of windows at a time instead of
     * pairwise.
     *
     * WARNING: please ensure stream has been flushed before calling */
    public CountBasedWBMH setWindowsPerMergeBatch(long W) {
        merger.setWindowsPerMergeBatch(W);
        return this;
    }

    /**
     * Parallelize issuing merges in each stream's Merger thread. We use Java's parallel streams, which uses a single
     * shared global work queue for all operations across all streams. */
    public CountBasedWBMH setParallelizeMerge(int nThreads) {
        //FIXME: global setting. Should switch to dedicated ExecutorService
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", Integer.toString(nThreads));
        merger.setParallelizeMerge(true);
        return this;
    }

    private void destroyEmptyBuffers() {
        for (IngestBuffer buffer : emptyBuffers) {
            buffer.close();
        }
        emptyBuffers.clear();
    }

    public void populateTransientFields(StreamWindowManager windowManager) {
        this.windowManager = windowManager;
        if (bufferSize > 0) {
            summarizer.populateTransientFields(windowManager);
        }
        writer.populateTransientFields(windowManager);
        merger.populateTransientFields(windowManager);

        new Thread(summarizer, windowManager.streamID + "-summarizer").start();
        new Thread(writer, windowManager.streamID + "-writer").start();
        new Thread(merger, windowManager.streamID + "-merger").start();
    }

    public void append(long ts, Object value) throws BackingStoreException {
        if (bufferSize > 0) {
            if (N % 100_000_000 == 0) {
                logger.info("N = {}M: {} unwritten windows, {} unprocessed merges, {} unissued merges, {} empty buffers",
                        N / 1_000_000,
                        writerQueue.size(), mergerQueue.size(), merger.getNumUnissuedMerges(), emptyBuffers.size());
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
        //assert sizeOfNewestWindow == 1;
        SummaryWindow newWindow = windowManager.createEmptySummaryWindow(timestamp, timestamp, N, N);
        windowManager.insertIntoSummaryWindow(newWindow, timestamp, value);
        windowManager.putSummaryWindow(newWindow);
        Utilities.put(writerQueue, newWindow);
        //Utilities.put(mergerQueue, new Merger.WindowInfo(timestamp, 1L));
    }

    private void flush(boolean shutdown, boolean setUnbuffered) throws BackingStoreException {
        long threshold = flushBarrier.getNextFlushThreshold();
        ingester.flush(shutdown);
        flushBarrier.wait(FlushBarrier.SUMMARIZER, threshold);
        if (bufferSize > 0) {
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
        if (setUnbuffered) {
            bufferSize = 0;
            destroyEmptyBuffers();
        }
        Utilities.put(writerQueue, shutdown ? Writer.SHUTDOWN_SENTINEL : Writer.FLUSH_SENTINEL);
        flushBarrier.wait(FlushBarrier.WRITER, threshold);
        Utilities.put(mergerQueue, shutdown ? Merger.SHUTDOWN_SENTINEL : Merger.FLUSH_SENTINEL);
        flushBarrier.wait(FlushBarrier.MERGER, threshold);
    }

    public void flush() throws BackingStoreException {
        flush(false, false);
    }

    /** flush and set buffer size to zero. TODO: document better and integrate into SummaryStore.flush() */
    public void flushAndSetUnbuffered() throws BackingStoreException {
        flush(false, true);
    }

    public void close() throws BackingStoreException {
        flush(true, false);
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
