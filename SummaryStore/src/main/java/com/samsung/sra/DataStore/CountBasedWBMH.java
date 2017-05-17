package com.samsung.sra.DataStore;

import com.samsung.sra.DataStore.Storage.BackingStoreException;
import com.samsung.sra.DataStore.Storage.StreamWindowManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teneighty.heap.FibonacciHeap;
import org.teneighty.heap.Heap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CountBasedWBMH implements WindowingMechanism {
    private static Logger logger = LoggerFactory.getLogger(CountBasedWBMH.class);
    private final Windowing windowing;

    private long lastSWID = -1;
    private long N = 0;

    /* Priority queue, mapping each summary window w_i to the time at which w_{i+1} will be merged into it. Using a
     * an alternative to the Java Collections PriorityQueue supporting efficient arbitrary-element delete.
     *
     * Why this particular pri-queue implementation?
     * https://gabormakrai.wordpress.com/2015/02/11/experimenting-with-dijkstras-algorithm/
     */
    private final FibonacciHeap<Long, Long> mergeCounts = new FibonacciHeap<>();
    private transient HashMap<Long, Heap.Entry<Long, Long>> heapEntries = new HashMap<>();

    private transient ExecutorService executorService;

    private static class IngestBuffer implements Serializable {
        private long[] timestamps;
        private Object[][] values;
        private int capacity = 0;
        private int size = 0;

        IngestBuffer(int capacity) {
            this.capacity = capacity;
            this.timestamps = new long[capacity];
            this.values = new Object[capacity][];
        }

        public void append(long ts, Object[] value) {
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

        int capacity() {
            return capacity;
        }

        void clear() {
            size = 0;
        }

        long getTimestamp(int pos) {
            if (pos < 0 || pos >= size) throw new IndexOutOfBoundsException();
            return timestamps[pos];
        }

        Object[] getValue(int pos) {
            if (pos < 0 || pos >= size) throw new IndexOutOfBoundsException();
            return values[pos];
        }
    }

    private long bufferSize;
    private final List<Long> bufferWindowLengths;

    /* We keep two ingest buffers, and at most two active threads at any given point:
         one thread flushing a full buffer,
         the other thread appending to the other buffer (until it becomes full)
     */
    private final BlockingQueue<IngestBuffer> emptyBuffers;
    private IngestBuffer activeBuffer = null; // FIXME: volatile?
    private Lock flushLock = new ReentrantLock();


    /**
     * Buffers up to numValuesToBuffer elements in memory, deferring window merges
     * until either the buffer fills up or flush() is called
     */
    public CountBasedWBMH(Windowing windowing, int numValuesToBuffer) {
        this.windowing = windowing;

        bufferWindowLengths = windowing.getWindowsCoveringUpto(numValuesToBuffer);
        bufferSize = bufferWindowLengths.stream().mapToLong(Long::longValue).sum();
        if (bufferSize > 0) {
            emptyBuffers = new ArrayBlockingQueue<>(2);
            emptyBuffers.add(new IngestBuffer((int)bufferSize));
            emptyBuffers.add(new IngestBuffer((int)bufferSize));
        } else {
            emptyBuffers = null;
        }
        logger.info("Buffer covers {} windows and {} values", bufferWindowLengths.size(), bufferSize);
    }

    public CountBasedWBMH(Windowing windowing) {
        this(windowing, 0);
    }

    @Override
    public void populateTransientFields(ExecutorService executorService) {
        this.executorService = executorService;
        heapEntries = new HashMap<>();
        for (Heap.Entry<Long, Long> entry: mergeCounts) {
            heapEntries.put(entry.getValue(), entry);
        }
    }

    @Override
    public void append(StreamWindowManager windows, long ts, Object[] value) throws BackingStoreException {
        //logger.debug("Appending in " + (numWindowsInBuffer==0? "Unbuffered":"buffered"));
        //if (numWindowsInBuffer == 0) {
        if (bufferSize == 0) {
            appendUnbuffered(windows, ts, value);
        } else {
            appendBuffered(windows, ts, value);
        }
    }

    @Override
    public void close(StreamWindowManager manager) throws BackingStoreException {
        if (bufferSize > 0) flush(manager);
    }

    private void appendUnbuffered(StreamWindowManager windows, long timestamp, Object[] value) throws BackingStoreException {
        // merge existing windows
        processMergesUntil(windows, N + 1);

        // insert newest element, creating a new window for it if necessary
        SummaryWindow lastWindow = lastSWID == -1 ? null : windows.getSummaryWindow(lastSWID);
        if (lastWindow != null && lastWindow.ce - lastWindow.cs + 1 < windowing.getSizeOfFirstWindow()) {
            // last window isn't yet full; insert new value into it
            lastWindow.ce = N;
            lastWindow.te = timestamp;
            windows.insertIntoSummaryWindow(lastWindow, timestamp, value);
            windows.putSummaryWindow(lastWindow);
        } else {
            // create new window holding the latest element
            //long newSWID = lastSWID + 1;
            SummaryWindow newWindow = windows.createEmptySummaryWindow(timestamp, timestamp, N, N, lastSWID, -1);
            windows.insertIntoSummaryWindow(newWindow, timestamp, value);
            windows.putSummaryWindow(newWindow);
            if (lastWindow != null) {
                lastWindow.nextTS = timestamp;
                updateMergeCountFor(lastWindow, newWindow, N + 1);
                windows.putSummaryWindow(lastWindow);
            }
            lastSWID = timestamp;
        }

        ++N;
    }

    /*NOTE: code here depends on the fact that append()/flush()/close() calls are serialized (by Stream).
            Else we would need more careful synchronization */
    private void appendBuffered(StreamWindowManager windows, long ts, Object[] value) throws BackingStoreException {
        while (activeBuffer == null) {
            try {
                activeBuffer = emptyBuffers.take();
            } catch (InterruptedException ignored) {
            }
        }
        assert !activeBuffer.isFull();
        activeBuffer.append(ts, value);
        if (activeBuffer.isFull()) { // buffer is full, flush
            // 1. Start flushing current active buffer in a different thread
            IngestBuffer flushingBuffer = activeBuffer;
            executorService.submit(() -> {
                flushLock.lock();
                try {
                    flushFullBuffer(windows, flushingBuffer);
                    flushingBuffer.clear();
                    boolean offered = emptyBuffers.offer(flushingBuffer);
                    assert offered;
                } catch (BackingStoreException e) {
                    throw new RuntimeException(e);
                } finally {
                    flushLock.unlock();
                }
            });
            // 2. Deactivate current buffer. Next call to append() will wait for an empty buffer to activate
            activeBuffer = null;
        }
    }

    @Override
    public void flush(StreamWindowManager windows) throws BackingStoreException {
        if (bufferSize == 0 || activeBuffer == null) return;
        flushLock.lock();
        try {
            if (activeBuffer.isFull()) {
                flushFullBuffer(windows, activeBuffer);
            } else { // append elements one by one
                for (int i = 0; i < activeBuffer.size(); ++i) {
                    appendUnbuffered(windows, activeBuffer.getTimestamp(i), activeBuffer.getValue(i));
                }
            }
            activeBuffer.clear();
            boolean offered = emptyBuffers.offer(activeBuffer);
            assert offered;
        } finally {
            flushLock.unlock();
        }
        activeBuffer = null;
    }

    /**
     * Set mergeCounts[(b1, b2)] = first N' >= N such that (b0, b1) will need to be
     *                             merged after N' elements have been inserted
     */
    private void updateMergeCountFor(SummaryWindow b0, SummaryWindow b1, long N) {
        if (b0 == null || b1 == null) return;

        Heap.Entry<Long, Long> existingEntry = heapEntries.remove(b0.ts);
        if (existingEntry != null) mergeCounts.delete(existingEntry);

        long newMergeCount = windowing.getFirstContainingTime(b0.cs, b1.ce, N);
        if (newMergeCount != -1) {
            heapEntries.put(b0.ts, mergeCounts.insert(newMergeCount, b0.ts));
        }
    }


    /** Advance count marker to N, apply the WBMH test, process any merges that result */
    private void processMergesUntil(StreamWindowManager windows, long N) throws BackingStoreException {
        while (!mergeCounts.isEmpty() && mergeCounts.getMinimum().getKey() <= N) {
            Heap.Entry<Long, Long> entry = mergeCounts.extractMinimum();
            Heap.Entry<Long, Long> removed = heapEntries.remove(entry.getValue());
            assert removed == entry;
            SummaryWindow b0 = windows.getSummaryWindow(entry.getValue());
            // We will now merge b0's successor b1 into b0. We also need to update b{-1}'s and
            // b2's prev and next pointers and b{-1} and b0's heap entries
            assert b0.nextTS != -1;
            SummaryWindow b1 = windows.deleteSummaryWindow(b0.nextTS);
            SummaryWindow b2 = b1.nextTS == -1 ? null : windows.getSummaryWindow(b1.nextTS);
            SummaryWindow bm1 = b0.prevTS == -1 ? null : windows.getSummaryWindow(b0.prevTS); // b{-1}

            windows.mergeSummaryWindows(b0, b1);

            if (bm1 != null) bm1.nextTS = b0.ts;
            b0.nextTS = b1.nextTS;
            if (b2 != null) b2.prevTS = b0.ts;
            if (b1.ts == lastSWID) lastSWID = b0.ts;

            Heap.Entry<Long, Long> b1entry = heapEntries.remove(b1.ts);
            if (b1entry != null) mergeCounts.delete(b1entry);
            updateMergeCountFor(bm1, b0, N);
            updateMergeCountFor(b0, b2, N);

            if (bm1 != null) windows.putSummaryWindow(bm1);
            windows.putSummaryWindow(b0);
            if (b2 != null) windows.putSummaryWindow(b2);
        }
    }

    private void flushFullBuffer(StreamWindowManager windows, IngestBuffer buffer) throws BackingStoreException {
        assert bufferSize > 0 && buffer.isFull();

        long lastExtantWindowID = lastSWID;
        SummaryWindow lastExtantWindow;
        if (lastExtantWindowID != -1) {
            lastExtantWindow = windows.getSummaryWindow(lastExtantWindowID);
            lastExtantWindow.nextTS = buffer.getTimestamp(0);
        } else {
            lastExtantWindow = null;
        }
        SummaryWindow[] newWindows = new SummaryWindow[bufferWindowLengths.size()];

        // create new windows
        {
            long cBase = (lastExtantWindow == null) ? 0 : lastExtantWindow.ce + 1;
            int cStartOffset = 0, cEndOffset;
            for (int wNum = 0; wNum < newWindows.length; ++wNum) {
                int wSize = bufferWindowLengths.get(newWindows.length - 1 - wNum).intValue();
                cEndOffset = cStartOffset + wSize - 1;

                SummaryWindow window = windows.createEmptySummaryWindow(
                        buffer.getTimestamp(cStartOffset),
                        buffer.getTimestamp(cEndOffset),
                        cBase + cStartOffset,
                        cBase + cEndOffset,
                        wNum == 0 ? lastExtantWindowID : newWindows[wNum-1].ts,
                        wNum == newWindows.length - 1 ? -1 : buffer.getTimestamp(cEndOffset + 1));
                if (wNum > 0) assert newWindows[wNum-1].nextTS == window.ts;
                for (int c = cStartOffset; c <= cEndOffset; ++c) {
                    windows.insertIntoSummaryWindow(window, buffer.getTimestamp(c), buffer.getValue(c));
                }
                newWindows[wNum] = window;

                cStartOffset += wSize;
            }
        }

        // update merge counts for the new window
        if (lastExtantWindow != null) updateMergeCountFor(lastExtantWindow, newWindows[0], N + bufferSize);
        for (int b = 0; b < newWindows.length - 1; ++b) {
            updateMergeCountFor(newWindows[b], newWindows[b+1], N + bufferSize);
        }

        // insert all modified windows into backing store
        if (lastExtantWindow != null) windows.putSummaryWindow(lastExtantWindow);
        for (SummaryWindow window: newWindows) windows.putSummaryWindow(window);

        // insert complete, advance counter
        N += bufferSize;
        lastSWID = newWindows[newWindows.length-1].ts;

        // process any pending merges (should all be in the older windows)
        processMergesUntil(windows, N);
    }
}
