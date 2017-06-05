package com.samsung.sra.DataStore.Storage;

import com.samsung.sra.DataStore.LandmarkWindow;
import com.samsung.sra.DataStore.SummaryWindow;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

public class MainMemoryBackingStore extends BackingStore {
    private Map<Long, ConcurrentSkipListMap<Long, SummaryWindow>> summaryWindows = new ConcurrentHashMap<>();
    private Map<Long, ConcurrentSkipListMap<Long, LandmarkWindow>> landmarkWindows = new ConcurrentHashMap<>();

    @Override
    SummaryWindow getSummaryWindow(StreamWindowManager windowManager, long swid) {
        return summaryWindows.get(windowManager.streamID).get(swid);
    }

    /**
     * Return all windows with swid (= window start timestamp) overlapping [t0, t1].  Specifically, if window start
     * times are the wi in
     *    ===== w5 ======== w6 ======== w7 ======== w8 =====
     *                |                        |
     *               t0                       t1
     * return [w5, w6, w7]
     */
    @Override
    Stream<SummaryWindow> getSummaryWindowsOverlapping(StreamWindowManager windowManager, long t0, long t1) {
        ConcurrentSkipListMap<Long, SummaryWindow> windows = summaryWindows.get(windowManager.streamID);
        if (windows == null || windows.isEmpty()) {
            return Stream.empty();
        }
        Long l = windows.floorKey(t0);
        Long r = windows.higherKey(t1);
        if (l == null) {
            l = windows.firstKey();
        }
        if (r == null) {
            r = windows.lastKey() + 1;
        }
        return windows.subMap(l, true, r, false).values().stream();
    }

    @Override
    SummaryWindow deleteSummaryWindow(StreamWindowManager windowManager, long swid) {
        return summaryWindows.get(windowManager.streamID).remove(swid);
    }

    @Override
    void putSummaryWindow(StreamWindowManager windowManager, long swid, SummaryWindow window) {
        ConcurrentSkipListMap<Long, SummaryWindow> stream = summaryWindows.get(windowManager.streamID);
        if (stream == null) {
            summaryWindows.put(windowManager.streamID, (stream = new ConcurrentSkipListMap<>()));
        }
        stream.put(swid, window);
    }

    @Override
    long getNumSummaryWindows(StreamWindowManager windowManager) {
        return summaryWindows.get(windowManager.streamID).size();
    }

    @Override
    LandmarkWindow getLandmarkWindow(StreamWindowManager windowManager, long lwid) {
        return landmarkWindows.get(windowManager.streamID).get(lwid);
    }

    @Override
    void putLandmarkWindow(StreamWindowManager windowManager, long lwid, LandmarkWindow window) {
        ConcurrentSkipListMap<Long, LandmarkWindow> stream = landmarkWindows.get(windowManager.streamID);
        if (stream == null) {
            landmarkWindows.put(windowManager.streamID, (stream = new ConcurrentSkipListMap<>()));
        }
        stream.put(lwid, window);
    }

    @Override
    void printWindowState(StreamWindowManager windowManager) {
        System.out.println("Stream " + windowManager.streamID);
        printWindowMap(summaryWindows.get(windowManager.streamID), "summary");
        printWindowMap(landmarkWindows.get(windowManager.streamID), "landmark");
    }

    private static void printWindowMap(Map map, String windowType) {
        if (map == null) {
            System.out.printf("\t0 %s windows\n", windowType);
        } else {
            System.out.printf("\t%d %s windows\n", map.size(), windowType);
            for (Object w: map.values()) {
                System.out.println("\t" + w);
            }
        }
    }

    @Override
    public void close() {
        summaryWindows.clear();
    }
}
