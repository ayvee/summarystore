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
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teneighty.heap.FibonacciHeap;
import org.teneighty.heap.Heap;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;

/**
 * Implements WBMH using a heap data structure to track pending merges. Batches merges and initiates merge ops in backing
 * store once every windowsPerBatch window inserts */
class HeapMerger extends Merger {
    private static final Logger logger = LoggerFactory.getLogger(Merger.class);

    // notifications of new window creates: a pair of (window ID, window size)
    private final BlockingQueue<Merger.WindowInfo> newWindowNotifications; // input queue
    private final CountBasedWBMH.FlushBarrier flushBarrier;

    private final Windowing windowing;

    private transient StreamWindowManager windowManager;

    private long windowsPerBatch;
    private boolean parallelizeMerge;

    private long N = 0, W = 0; // number of elements in stream, number of raw windows ingested (without merging)
    /* Priority queue, mapping each summary window w_i to the time at which w_{i+1} will be merged into it. Using
     * an alternative to the Java Collections PriorityQueue supporting efficient arbitrary-element delete.
     *
     * Why this particular pri-queue implementation?
     * https://gabormakrai.wordpress.com/2015/02/11/experimenting-with-dijkstras-algorithm/
     */
    private final FibonacciHeap<Long, Long> mergeCounts = new FibonacciHeap<>();
    private final WindowInfo windowInfo = new WindowInfo();

    HeapMerger(Windowing windowing, BlockingQueue<Merger.WindowInfo> newWindowNotifications,
               CountBasedWBMH.FlushBarrier flushBarrier,
               long windowsPerBatch) {
        this.windowing = windowing;
        this.newWindowNotifications = newWindowNotifications;
        this.flushBarrier = flushBarrier;
        this.windowsPerBatch = windowsPerBatch;
    }

    void setWindowsPerMergeBatch(long windowsPerMergeBatch) {
        this.windowsPerBatch = windowsPerMergeBatch;
    }

    void setParallelizeMerge(boolean parallelizeMerge) {
        this.parallelizeMerge = parallelizeMerge;
    }

    @Override
    public void populateTransientFields(StreamWindowManager windowManager) {
        this.windowManager = windowManager;
        windowInfo.populateTransientFields(mergeCounts);
    }

    @Override
    public void run() {
        try {
            while (true) {
                Merger.WindowInfo newWindow = Utilities.take(newWindowNotifications);
                if (newWindow == SHUTDOWN_SENTINEL) {
                    issueAllPendingMerges();
                    logger.info("Merge chain length: avg = {}, range = [{}, {}]",
                            String.format("%.1f", mergeLengthStats.getMean()), mergeLengthStats.getMin(), mergeLengthStats.getMax());
                    flushBarrier.notify(CountBasedWBMH.FlushBarrier.MERGER);
                    break;
                } else if (newWindow == FLUSH_SENTINEL) {
                    issueAllPendingMerges();
                    flushBarrier.notify(CountBasedWBMH.FlushBarrier.MERGER);
                    continue;
                }
                long newWindowID = newWindow.id, newWindowSize = newWindow.size;
                N += newWindowSize;
                Long lastWindowID = windowInfo.getLastSWID();
                if (lastWindowID != null) {
                    updateMergeCountFor(lastWindowID, newWindowID, windowInfo.getCStart(lastWindowID), N - 1, N);
                }
                windowInfo.put(newWindowID, N - 1);
                updatePendingMerges();
                if (++W % windowsPerBatch == 0) {
                    issueAllPendingMerges();
                }
            }
        } catch (BackingStoreException e) {
            throw new RuntimeException(e);
        }
    }

    private void updatePendingMerges() {
        while (!mergeCounts.isEmpty() && mergeCounts.getMinimum().getKey() <= N) {
            Heap.Entry<Long, Long> entry = mergeCounts.extractMinimum();
            Heap.Entry<Long, Long> removed = windowInfo.unsetHeapPtr(entry.getValue());
            assert entry == removed;
            // We will now merge w0's successor w1 into w0, and update the heap entries for w{-1} and w0
            Long w0ID = entry.getValue();
            Long w1ID = windowInfo.getSuccessor(w0ID);
            assert windowInfo.containsSWID(w0ID) && windowInfo.containsSWID(w1ID);
            Long wm1ID = windowInfo.getPredecessor(w0ID);
            Long w2ID = windowInfo.getSuccessor(w1ID);
            long newW0cs = windowInfo.getCStart(w0ID);
            long newW0ce = windowInfo.getCEnd(w1ID);

            addPendingMerge(w0ID, w1ID);

            WindowInfo.Info oldW1info = windowInfo.remove(w1ID);
            windowInfo.put(w0ID, newW0ce);

            if (oldW1info.heapPtr != null) mergeCounts.delete(oldW1info.heapPtr);
            updateMergeCountFor(wm1ID, w0ID, windowInfo.getCStart(wm1ID), newW0ce, N);
            updateMergeCountFor(w0ID, w2ID, newW0cs, windowInfo.getCEnd(w2ID), N);
        }
    }

    private final Map<Long, List<Long>> pendingMerges = new Long2ObjectOpenHashMap<>();

    /* add entry merge(swid0, [any windows already merged into swid0], swid1, [any windows alread merged into swid1]) */
    private void addPendingMerge(long swid0, long swid1) {
        List<Long> tail = pendingMerges.get(swid0);
        if (tail == null) tail = new LongArrayList();
        tail.add(swid1);
        List<Long> transitiveTail = pendingMerges.remove(swid1);
        if (transitiveTail != null) tail.addAll(transitiveTail);
        pendingMerges.put(swid0, tail);
    }

    private final DescriptiveStatistics mergeLengthStats = new DescriptiveStatistics();

    private void issuePendingMerge(Map.Entry<Long, List<Long>> entry) throws BackingStoreException {
        long head = entry.getKey();
        List<Long> tail = entry.getValue();
        assert !tail.isEmpty();
        mergeLengthStats.addValue(1 + tail.size());
        SummaryWindow[] windows = new SummaryWindow[1 + tail.size()];
        int w = 0;
        windows[w++] = windowManager.getSummaryWindow(head);
        for (long swid : tail) {
            windows[w++] = windowManager.getSummaryWindow(swid);
        }
        assert w == windows.length;
        windowManager.mergeSummaryWindows(windows);
        windowManager.putSummaryWindow(windows[0]);
        for (long swid : tail) {
            windowManager.deleteSummaryWindow(swid);
        }
    }

    private void issueAllPendingMerges() throws BackingStoreException {
        Stream<Map.Entry<Long, List<Long>>> stream = pendingMerges.entrySet().stream();
        if (parallelizeMerge) {
            stream = stream.parallel();
        }
        stream.forEach(entry -> {
            try {
                issuePendingMerge(entry);
            } catch (BackingStoreException e) {
                throw new RuntimeException(e);
            }
        });
        pendingMerges.clear();
        ((Long2ObjectOpenHashMap) pendingMerges).trim();
    }

    /**
     * Given consecutive windows w0, w1 which together span the count range [c0, c1], set
     * mergeCounts[(w0, w1)] = first N' >= N such that (w0, w1) will need to be merged after N' elements have been
     *                         inserted
     */
    private void updateMergeCountFor(Long w0ID, Long w1ID, Long c0, Long c1, long N) {
        if (w0ID == null || w1ID == null || c0 == null || c1 == null) return;
        Heap.Entry<Long, Long> existingEntry = windowInfo.unsetHeapPtr(w0ID);
        if (existingEntry != null) mergeCounts.delete(existingEntry);

        long newMergeCount = windowing.getFirstContainingTime(c0, c1, N);
        if (newMergeCount != -1) {
            windowInfo.setHeapPtr(w0ID, mergeCounts.insert(newMergeCount, w0ID));
        }
    }

    long getNumUnissuedMerges() {
        return pendingMerges.size();
    }

    /** In-memory index allowing looking up various info given SWID */
    private static class WindowInfo implements Serializable {
        private static class Info implements Serializable {
            long cEnd;
            transient Heap.Entry<Long, Long> heapPtr;

            Info(long cEnd, Heap.Entry<Long, Long> heapPtr) {
                this.cEnd = cEnd;
                this.heapPtr = heapPtr;
            }
        }

        private final TreeMap<Long, Info> info = new TreeMap<>(); // end timestamp of each window

        private void populateTransientFields(Heap<Long, Long> heap) {
            for (Heap.Entry<Long, Long> entry: heap) {
                info.get(entry.getValue()).heapPtr = entry;
            }
        }

        private void put(long swid, long cEnd){
            info.put(swid, new Info(cEnd, null));
        }

        private Info remove(long swid) {
            return info.remove(swid);
        }

        private boolean containsSWID(Long swid) {
            return swid != null && info.containsKey(swid);
        }

        private Long getCStart(Long swid) {
            if (swid == null || !info.containsKey(swid)) return null;
            Map.Entry<Long, Info> prevEntry = info.lowerEntry(swid);
            if (prevEntry != null) {
                return prevEntry.getValue().cEnd + 1;
            } else {
                assert swid.equals(info.firstKey());
                return 0L;
            }
        }

        private Long getCEnd(Long swid) {
            if (swid == null) return null;
            Info w = info.get(swid);
            return w != null ? w.cEnd : null;
        }

        private Long getPredecessor(Long swid) {
            if (swid == null || !info.containsKey(swid)) return null;
            return info.lowerKey(swid);
        }

        private Long getSuccessor(Long swid) {
            if (swid == null || !info.containsKey(swid)) return null;
            return info.higherKey(swid);
        }

        private Long getLastSWID() {
            return !info.isEmpty() ? info.lastKey() : null;
        }

        private Heap.Entry<Long, Long> unsetHeapPtr(long swid) {
            Info i = info.get(swid);
            Heap.Entry<Long, Long> ptr = i.heapPtr;
            i.heapPtr = null;
            return ptr;
        }

        private void setHeapPtr(long swid, Heap.Entry<Long, Long> ptr) {
            info.get(swid).heapPtr = ptr;
        }
    }
}
