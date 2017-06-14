package com.samsung.sra.DataStore.Storage;

import com.samsung.sra.DataStore.LandmarkWindow;
import com.samsung.sra.DataStore.SummaryWindow;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MainMemoryBackingStore extends BackingStore {
    private Map<Long, ConcurrentSkipListMap<Long, SummaryWindow>> summaryWindows = new ConcurrentHashMap<>();
    private Map<Long, ConcurrentSkipListMap<Long, LandmarkWindow>> landmarkWindows = new ConcurrentHashMap<>();

    @Override
    SummaryWindow getSummaryWindow(StreamWindowManager windowManager, long swid) {
        return summaryWindows.get(windowManager.streamID).get(swid);
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
