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
    SummaryWindow getSummaryWindow(long streamID, long swid, SerDe serDe) {
        return summaryWindows.get(streamID).get(swid);
    }

    @Override
    Stream<SummaryWindow> getSummaryWindowsOverlapping(long streamID, long t0, long t1, SerDe serDe) throws BackingStoreException {
        ConcurrentSkipListMap<Long, SummaryWindow> windows = summaryWindows.get(streamID);
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
    void deleteSummaryWindow(long streamID, long swid, SerDe serDe) {
        summaryWindows.get(streamID).remove(swid);
    }

    @Override
    void putSummaryWindow(long streamID, long swid, SerDe serDe, SummaryWindow window) {
        ConcurrentSkipListMap<Long, SummaryWindow> stream = summaryWindows.get(streamID);
        if (stream == null) {
            summaryWindows.put(streamID, (stream = new ConcurrentSkipListMap<>()));
        }
        stream.put(swid, window);
    }

    @Override
    long getNumSummaryWindows(long streamID, SerDe serDe) throws BackingStoreException {
        return summaryWindows.get(streamID).size();
    }

    @Override
    LandmarkWindow getLandmarkWindow(long streamID, long lwid, SerDe serDe) {
        return landmarkWindows.get(streamID).get(lwid);
    }

    @Override
    void putLandmarkWindow(long streamID, long lwid, SerDe serDe, LandmarkWindow window) {
        ConcurrentSkipListMap<Long, LandmarkWindow> stream = landmarkWindows.get(streamID);
        if (stream == null) landmarkWindows.put(streamID, (stream = new ConcurrentSkipListMap<>()));
        stream.put(lwid, window);
    }

    /*@Override
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
    }*/

    @Override
    public void close() {
        summaryWindows.clear();
    }
}
