package com.samsung.sra.DataStore.Ingest;

import com.samsung.sra.DataStore.Storage.BackingStoreException;
import com.samsung.sra.DataStore.Storage.StreamWindowManager;
import com.samsung.sra.DataStore.SummaryWindow;
import com.samsung.sra.DataStore.Utilities;
import com.samsung.sra.DataStore.Windowing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teneighty.heap.FibonacciHeap;
import org.teneighty.heap.Heap;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;

/** Implement WBMH using a heap data structure to track pending merges */
class HeapMerger implements Merger {
    static final WindowInfo SHUTDOWN_SENTINEL = new WindowInfo(-1L, -1L);
    static final WindowInfo FLUSH_SENTINEL = new WindowInfo(-1L, -1L);
    private static final Logger logger = LoggerFactory.getLogger(Merger.class);

    // notifications of new window creates: a pair of (window ID, window size)
    private final BlockingQueue<WindowInfo> newWindowNotifications; // input queue
    private final CountBasedWBMH.FlushBarrier flushBarrier;

    private final Windowing windowing;

    private transient StreamWindowManager windowManager;

    private long N = 0;
    /* Priority queue, mapping each summary window w_i to the time at which w_{i+1} will be merged into it. Using
     * an alternative to the Java Collections PriorityQueue supporting efficient arbitrary-element delete.
     *
     * Why this particular pri-queue implementation?
     * https://gabormakrai.wordpress.com/2015/02/11/experimenting-with-dijkstras-algorithm/
     */
    private final FibonacciHeap<Long, Long> mergeCounts = new FibonacciHeap<>();
    private transient HashMap<Long, Heap.Entry<Long, Long>> heapEntries = new HashMap<>();

    private final TreeMap<Long, Long> windowCEnds = new TreeMap<>(); // end timestamp of each window

    private Long getCEnd(Long windowID) {
        if (windowID == null || !windowCEnds.containsKey(windowID)) return null;
        return windowCEnds.get(windowID);
    }

    private Long getCStart(Long windowID) {
        if (windowID == null || !windowCEnds.containsKey(windowID)) return null;
        Map.Entry<Long, Long> prevEntry = windowCEnds.lowerEntry(windowID);
        if (prevEntry != null) {
            return prevEntry.getValue() + 1;
        } else {
            assert windowID.equals(windowCEnds.firstKey());
            return 0L;
        }
    }

    HeapMerger(Windowing windowing, BlockingQueue<WindowInfo> newWindowNotifications, CountBasedWBMH.FlushBarrier flushBarrier) {
        this.windowing = windowing;
        this.newWindowNotifications = newWindowNotifications;
        this.flushBarrier = flushBarrier;
    }

    @Override
    public void populateTransientFields(StreamWindowManager windowManager) {
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
                WindowInfo newWindow = Utilities.take(newWindowNotifications);
                if (newWindow == SHUTDOWN_SENTINEL) {
                    flushBarrier.notify(CountBasedWBMH.FlushBarrier.MERGER);
                    break;
                } else if (newWindow == FLUSH_SENTINEL) {
                    flushBarrier.notify(CountBasedWBMH.FlushBarrier.MERGER);
                    continue;
                }
                long newWindowID = newWindow.id, newWindowSize = newWindow.size;
                N += newWindowSize;
                if (!windowCEnds.isEmpty()) {
                    long lastWindowID = windowCEnds.lastKey();
                    updateMergeCountFor(lastWindowID, newWindowID, getCStart(lastWindowID), N - 1, N);
                }
                windowCEnds.put(newWindowID, N - 1);
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
            Long w1ID = windowCEnds.higherKey(w0ID);
            assert w0ID != null && windowCEnds.containsKey(w0ID) && w1ID != null && windowCEnds.containsKey(w1ID);
            Long wm1ID = windowCEnds.lowerKey(w0ID);
            Long w2ID = windowCEnds.higherKey(w1ID);

            SummaryWindow w0 = windowManager.getSummaryWindow(w0ID);
            SummaryWindow w1 = windowManager.deleteSummaryWindow(w1ID);
            windowManager.mergeSummaryWindows(w0, w1);
            windowManager.putSummaryWindow(w0);

            windowCEnds.remove(w1ID);
            windowCEnds.put(w0ID, w0.ce);

            Heap.Entry<Long, Long> w1entry = heapEntries.remove(w1ID);
            if (w1entry != null) mergeCounts.delete(w1entry);
            updateMergeCountFor(wm1ID, w0ID, getCStart(wm1ID), w0.ce, N);
            updateMergeCountFor(w0ID, w2ID, w0.cs, getCEnd(w2ID), N);
        }
    }

    /**
     * Given consecutive windows w0, w1 which together span the count range [c0, c1], set
     * mergeCounts[(w0, w1)] = first N' >= N such that (w0, w1) will need to be merged after N' elements have been
     *                         inserted
     */
    private void updateMergeCountFor(Long w0ID, Long w1ID, Long c0, Long c1, long N) {
        if (w0ID == null || w1ID == null || c0 == null || c1 == null) return;
        Heap.Entry<Long, Long> existingEntry = heapEntries.remove(w0ID);
        if (existingEntry != null) mergeCounts.delete(existingEntry);

        long newMergeCount = windowing.getFirstContainingTime(c0, c1, N);
        if (newMergeCount != -1) {
            heapEntries.put(w0ID, mergeCounts.insert(newMergeCount, w0ID));
        }
    }
}
