package com.samsung.sra.DataStore.Storage;

import com.samsung.sra.DataStore.LandmarkWindow;
import com.samsung.sra.DataStore.SummaryWindow;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Basically a copy of MainMemoryBackingStore with publicly visible backing hashmaps. Used in test-suite */
public class PeekableBackingStore extends BackingStore {
    public Map<Long, Map<Long, SummaryWindow>> summaryWindows = new ConcurrentHashMap<>();
    public Map<Long, Map<Long, LandmarkWindow>> landmarkWindows = new ConcurrentHashMap<>();

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
        Map<Long, SummaryWindow> stream = summaryWindows.get(windowManager.streamID);
        if (stream == null) {
            summaryWindows.put(windowManager.streamID, (stream = new ConcurrentHashMap<>()));
        }
        stream.put(swid, window);
    }

    @Override
    LandmarkWindow getLandmarkWindow(StreamWindowManager windowManager, long lwid) {
        return landmarkWindows.get(windowManager.streamID).get(lwid);
    }

    @Override
    void putLandmarkWindow(StreamWindowManager windowManager, long lwid, LandmarkWindow window) {
        Map<Long, LandmarkWindow> stream = landmarkWindows.get(windowManager.streamID);
        if (stream == null) {
            landmarkWindows.put(windowManager.streamID, (stream = new ConcurrentHashMap<>()));
        }
        stream.put(lwid, window);
    }

    @Override
    public void close() {
        summaryWindows.clear();
    }
}
