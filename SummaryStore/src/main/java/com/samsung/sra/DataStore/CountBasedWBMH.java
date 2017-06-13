package com.samsung.sra.DataStore;

import com.samsung.sra.DataStore.Storage.BackingStoreException;
import com.samsung.sra.DataStore.Storage.StreamWindowManager;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teneighty.heap.FibonacciHeap;
import org.teneighty.heap.Heap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
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

    private final BlockingQueue<WindowInfo> newWindowNotifications; // input queue for merger
    private final FlushHandler flushHandler;

    private transient Thread summarizerThread, writerThread, mergerThread;

    private long N = 0;

    /** Ingest values into initially empty buffers, and once buffers fill up move them to Summarizer's queue */
    private static class Ingester implements Serializable {
        private final BlockingQueue<IngestBuffer> emptyBuffers; // input queue
        private final BlockingQueue<IngestBuffer> buffersToSummarize; // output queue

        // WARNING: should be declared volatile if external user code appends to the same stream from more than one thread
        private IngestBuffer activeBuffer = null;

        private Ingester(BlockingQueue<IngestBuffer> emptyBuffers, BlockingQueue<IngestBuffer> buffersToSummarize) {
            this.emptyBuffers = emptyBuffers;
            this.buffersToSummarize = buffersToSummarize;
        }

        /** NOTE: must externally serialize all append() and close() */
        private void append(long ts, Object value) {
            while (activeBuffer == null) {
                try {
                    activeBuffer = emptyBuffers.take();
                } catch (InterruptedException ignored) {
                }
            }
            assert !activeBuffer.isFull();
            activeBuffer.append(ts, value);
            if (activeBuffer.isFull()) {
                offerAndConfirm(buffersToSummarize, activeBuffer);
                activeBuffer = null;
            }
        }

        /**
         * Initiate flush/shutdown of summarizer and parser, and if we have a partially filled buffer return it (as well
         * as the list of empty buffers) so that the caller can process its contents using appendUnbuffered.
         *
         * NOTE: must externally serialize all append() and close() */
        private Pair<IngestBuffer, BlockingQueue<IngestBuffer>> flush(boolean shutdown) {
            Pair<IngestBuffer, BlockingQueue<IngestBuffer>> ret;
            if (activeBuffer != null && activeBuffer.size() > 0) {
                assert !activeBuffer.isFull();
                ret = new Pair<>(activeBuffer, emptyBuffers);
                activeBuffer = null;
            } else {
                ret = new Pair<>(null, null);
            }
            // initiate graceful summarizer and writer shutdown
            offerAndConfirm(buffersToSummarize, shutdown ? Summarizer.SHUTDOWN_SENTINEL : Summarizer.FLUSH_SENTINEL);
            return ret;
        }
    }

    /** Summarize values in full IngestBuffers into a sequence of SummaryWindows, and pass them on to Writer's queue */
    private static class Summarizer implements Runnable, Serializable {
        private static final IngestBuffer SHUTDOWN_SENTINEL = new IngestBuffer(0);
        private static final IngestBuffer FLUSH_SENTINEL = new IngestBuffer(0);

        private final BlockingQueue<IngestBuffer> buffersToSummarize; // input queue
        private final BlockingQueue<SummaryWindow> windowsToWrite; // output queue
        private final BlockingQueue<IngestBuffer> emptyBuffers; // output queue (discards cleared out buffers here)
        private final FlushHandler flushHandler;

        private final int[] windowLengths;

        private long N = 0;

        private transient StreamWindowManager windowManager = null;

        private Summarizer(int[] windowLengths, BlockingQueue<IngestBuffer> emptyBuffers,
                           BlockingQueue<IngestBuffer> buffersToSummarize,
                           BlockingQueue<SummaryWindow> windowsToWrite,
                           FlushHandler flushHandler) {
            this.windowLengths = windowLengths;
            this.emptyBuffers = emptyBuffers;
            this.buffersToSummarize = buffersToSummarize;
            this.windowsToWrite = windowsToWrite;
            this.flushHandler = flushHandler;
        }

        private void populateTransientFields(StreamWindowManager windowManager) {
            this.windowManager = windowManager;
        }

        @Override
        public void run() {
                while (true) {
                    IngestBuffer buffer = take(buffersToSummarize);
                    if (buffer == SHUTDOWN_SENTINEL) {
                        offerAndConfirm(windowsToWrite, Writer.SHUTDOWN_SENTINEL);
                        flushHandler.notifySummarizerFlush();
                        break;
                    } else if (buffer == FLUSH_SENTINEL) {
                        offerAndConfirm(windowsToWrite, Writer.FLUSH_SENTINEL);
                        flushHandler.notifySummarizerFlush();
                        continue;
                    }
                    int bs = 0, be; // index of first and last elements in the buffer belonging to current window
                    for (int w = windowLengths.length - 1; w >= 0; --w) {
                        be = bs + windowLengths[w] - 1;
                        SummaryWindow window = windowManager.createEmptySummaryWindow(
                                buffer.getTimestamp(bs), buffer.getTimestamp(be), N + bs, N + be);
                        for (int c = bs; c <= be; ++c) {
                            windowManager.insertIntoSummaryWindow(window, buffer.getTimestamp(c), buffer.getValue(c));
                        }
                        offerAndConfirm(windowsToWrite, window);
                        bs = be + 1;
                    }
                    assert bs == buffer.size();
                    N += bs;

                    buffer.clear();
                    offerAndConfirm(emptyBuffers, buffer);
                }

        }
    }

    private static class WindowInfo implements Serializable {
        public final long id;
        public final long size;

        WindowInfo(long id, long size) {
            this.id = id;
            this.size = size;
        }
    }

    /** Receive SummaryWindows and write them into RocksDB */
    private static class Writer implements Runnable, Serializable {
        private static final SummaryWindow SHUTDOWN_SENTINEL = new SummaryWindow();
        private static final SummaryWindow FLUSH_SENTINEL = new SummaryWindow();

        private final BlockingQueue<SummaryWindow> windowsToWrite; // input queue
        private final BlockingQueue<WindowInfo> newWindowNotifications; // output queue, feeding into Merger
        private final FlushHandler flushHandler;

        private transient StreamWindowManager windowManager;

        private Writer(BlockingQueue<SummaryWindow> windowsToWrite, BlockingQueue<WindowInfo> newWindowNotifications,
                       FlushHandler flushHandler) {
            this.windowsToWrite = windowsToWrite;
            this.newWindowNotifications = newWindowNotifications;
            this.flushHandler = flushHandler;
        }

        private void populateTransientFields(StreamWindowManager windowManager) {
            this.windowManager = windowManager;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    SummaryWindow window = take(windowsToWrite);
                    if (window == SHUTDOWN_SENTINEL) {
                        flushHandler.notifyWriterFlush();
                        break;
                    } else if (window == FLUSH_SENTINEL) {
                        flushHandler.notifyWriterFlush();
                        continue;
                    }
                    windowManager.putSummaryWindow(window);
                    offerAndConfirm(newWindowNotifications, new WindowInfo(window.ts, window.ce - window.cs + 1));
                }
            } catch (BackingStoreException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class Merger implements Runnable, Serializable {
        private static final WindowInfo SHUTDOWN_SENTINEL = new WindowInfo(-1L, -1L);
        private static final WindowInfo FLUSH_SENTINEL = new WindowInfo(-1L, -1L);

        // notifications of new window creates: a pair of (window ID, window size)
        private final BlockingQueue<WindowInfo> newWindowNotifications; // input queue
        private final FlushHandler flushHandler;

        private final Windowing windowing;

        private transient StreamWindowManager windowManager;

        private long N = 0;
        /* Priority queue, mapping each summary window w_i to the time at which w_{i+1} will be merged into it. Using a
         * an alternative to the Java Collections PriorityQueue supporting efficient arbitrary-element delete.
         *
         * Why this particular pri-queue implementation?
         * https://gabormakrai.wordpress.com/2015/02/11/experimenting-with-dijkstras-algorithm/
         */
        private final FibonacciHeap<Long, Long> mergeCounts = new FibonacciHeap<>();
        private transient HashMap<Long, Heap.Entry<Long, Long>> heapEntries = new HashMap<>();
        private final TreeMap<Long, Long> windowSizes = new TreeMap<>();

        private Merger(Windowing windowing, BlockingQueue<WindowInfo> newWindowNotifications, FlushHandler flushHandler) {
            this.windowing = windowing;
            this.newWindowNotifications = newWindowNotifications;
            this.flushHandler = flushHandler;
        }

        private void populateTransientFields(StreamWindowManager windowManager) {
            this.windowManager = windowManager;
            heapEntries = new HashMap<>();
            for (Heap.Entry<Long, Long> entry: mergeCounts) {
                heapEntries.put(entry.getValue(), entry);
            }
        }

        @Override
        public void run() {
            try {
                while (true) {
                    WindowInfo newWindow = take(newWindowNotifications);
                    if (newWindow == SHUTDOWN_SENTINEL) {
                        flushHandler.notifyMergerFlush();
                        break;
                    } else if (newWindow == FLUSH_SENTINEL) {
                        flushHandler.notifyMergerFlush();
                        continue;
                    }
                    long newWindowID = newWindow.id, newWindowSize = newWindow.size;
                    if (!windowSizes.isEmpty()) {
                        Map.Entry<Long, Long> lastEntry = windowSizes.lastEntry();
                        long lastWindowID = lastEntry.getKey(), lastWindowSize = lastEntry.getValue();
                        // last window spans [N - lastWindowSize, N - 1], this window spans [N, N + newWindowSize - 1]
                        updateMergeCountFor(lastWindowID, newWindowID, N - lastWindowSize, N + newWindowSize - 1,
                                N + newWindowSize);
                    }
                    windowSizes.put(newWindowID, newWindowSize);
                    N += newWindowSize;
                    processPendingMerges();
                }
            } catch (BackingStoreException e) {
                throw new RuntimeException(e);
            }
        }


        private void processPendingMerges() throws BackingStoreException {
            while (!mergeCounts.isEmpty() && mergeCounts.getMinimum().getKey() <= N) {
                Heap.Entry<Long, Long> entry = mergeCounts.extractMinimum();
                Heap.Entry<Long, Long> removed = heapEntries.remove(entry.getValue());
                assert removed == entry;
                // We will now merge w0's successor w1 into w0, and update the heap entries for w{-1} and w0
                Long w0ID = entry.getValue();
                Long w1ID = windowSizes.higherKey(w0ID);
                assert w0ID != null && windowSizes.containsKey(w0ID) && w1ID != null && windowSizes.containsKey(w1ID);
                Long wm1ID = windowSizes.lowerKey(w0ID);
                Long w2ID = windowSizes.higherKey(w1ID);
                SummaryWindow w0 = windowManager.getSummaryWindow(w0ID);
                SummaryWindow w1 = windowManager.deleteSummaryWindow(w1ID);
                SummaryWindow w2 = w2ID == null ? null : windowManager.getSummaryWindow(w2ID);
                SummaryWindow wm1 = wm1ID == null ? null : windowManager.getSummaryWindow(wm1ID);

                windowManager.mergeSummaryWindows(w0, w1);
                windowManager.putSummaryWindow(w0);

                windowSizes.remove(w1ID);
                windowSizes.put(w0ID, w0.ce - w0.cs + 1);

                Heap.Entry<Long, Long> w1entry = heapEntries.remove(w1ID);
                if (w1entry != null) mergeCounts.delete(w1entry);
                if (wm1 != null) updateMergeCountFor(wm1ID, w0ID, wm1.cs, w0.ce, N);
                if (w2 != null) updateMergeCountFor(w0ID, w2ID, w0.cs, w2.ce, N);
            }
        }

        /**
         * Given consecutive windows w0, w1 which together span the count range [c0, c1], set
         * mergeCounts[(w0, w1)] = first N' >= N such that (w0, w1) will need to be merged after N' elements have been
         *                         inserted
         */
        private void updateMergeCountFor(long w0ID, long w1ID, long c0, long c1, long N) {
            Heap.Entry<Long, Long> existingEntry = heapEntries.remove(w0ID);
            if (existingEntry != null) mergeCounts.delete(existingEntry);

            long newMergeCount = windowing.getFirstContainingTime(c0, c1, N);
            if (newMergeCount != -1) {
                heapEntries.put(w0ID, mergeCounts.insert(newMergeCount, w0ID));
            }
        }
    }

    private static class FlushHandler implements Serializable {
        private final AtomicLong flushCount = new AtomicLong(0);
        private final Serializable monitor = new Object[0]; // any serializable object would do
        private long numSummarizerFlushes = 0, numWriterFlushes = 0, numMergerFlushes = 0;

        private FlushHandler() {
        }

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

        private void notifySummarizerFlush() {
            synchronized (monitor) {
                ++numSummarizerFlushes;
                monitor.notifyAll();
            }
        }

        private void notifyWriterFlush() {
            synchronized (monitor) {
                ++numWriterFlushes;
                monitor.notifyAll();
            }
        }

        private void notifyMergerFlush() {
            synchronized (monitor) {
                ++numMergerFlushes;
                monitor.notifyAll();
            }
        }
    }

    /** Identical to BlockingQueue.take, but handles InterruptedException instead of throwing */
    private static <T> T take(BlockingQueue<T> queue) {
        while (true) {
            try {
                return queue.take();
            } catch (InterruptedException ignored) {
            }
        }
    }

    private static <T> void offerAndConfirm(BlockingQueue<T> queue, T value) {
        boolean offered = queue.offer(value);
        assert offered;
    }

    /**
     * Buffers up to numValuesToBuffer elements in memory, deferring window merges
     * until either the buffer fills up or flush() is called
     */
    public CountBasedWBMH(Windowing windowing, int numValuesToBuffer) {
        this.sizeOfNewestWindow = windowing.getSizeOfFirstWindow();
        if (sizeOfNewestWindow > 1) {
            throw new NotImplementedException("do not yet support case when size of newest window > 1");
        }
        int[] bufferWindowLengths = windowing.getWindowsCoveringUpto(numValuesToBuffer)
                .stream().mapToInt(Long::intValue).toArray();
        bufferSize = IntStream.of(bufferWindowLengths).sum(); // actual buffer size, <= numValuesToBuffer
        logger.info("Ingest buffer covers {} windows and {} values", bufferWindowLengths.length, bufferSize);

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
        merger = new Merger(windowing, newWindowNotifications, flushHandler);
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
            summarizerThread = new Thread(summarizer);
            writerThread = new Thread(writer);
            summarizerThread.start();
            writerThread.start();
        }
        mergerThread = new Thread(merger);
        mergerThread.start();
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
        offerAndConfirm(newWindowNotifications, new WindowInfo(timestamp, 1L));
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
                offerAndConfirm(emptyBuffers, partlyFullBuffer);
            }
        }
        offerAndConfirm(newWindowNotifications, shutdown ? Merger.SHUTDOWN_SENTINEL : Merger.FLUSH_SENTINEL);
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

    /**
     * Fixed-size buffer of time-value pairs
     */
    private static class IngestBuffer implements Serializable {
        private long[] timestamps;
        private Object[] values;
        private final int capacity;
        private int size = 0;

        IngestBuffer(int capacity) {
            this.capacity = capacity;
            this.timestamps = new long[capacity];
            this.values = new Object[capacity];
        }

        public void append(long ts, Object value) {
            if (size >= capacity) throw new IndexOutOfBoundsException();
            timestamps[size] = ts;
            values[size] = value;
            ++size;
        }

        boolean isFull() {
            return size == capacity;
        }

        int size() {
            return size;
        }

        void clear() {
            size = 0;
        }

        long getTimestamp(int pos) {
            if (pos < 0 || pos >= size) throw new IndexOutOfBoundsException();
            return timestamps[pos];
        }

        Object getValue(int pos) {
            if (pos < 0 || pos >= size) throw new IndexOutOfBoundsException();
            return values[pos];
        }
    }
}
