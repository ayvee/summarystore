package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class MainMemoryBackingStore implements BackingStore {
    private Map<Long, Map<Long, SummaryWindow>> summaryWindows = new ConcurrentHashMap<>();
    private Map<Long, Map<Long, LandmarkWindow>> landmarkWindows = new ConcurrentHashMap<>();

    @Override
    public SummaryWindow getSummaryWindow(StreamManager streamManager, long swid) throws RocksDBException {
        return summaryWindows.get(streamManager.streamID).get(swid);
    }

    @Override
    public SummaryWindow deleteSummaryWindow(StreamManager streamManager, long swid) throws RocksDBException {
        return summaryWindows.get(streamManager.streamID).remove(swid);
    }

    @Override
    public void putSummaryWindow(StreamManager streamManager, long swid, SummaryWindow window) throws RocksDBException {
        Map<Long, SummaryWindow> stream = summaryWindows.get(streamManager.streamID);
        if (stream == null) {
            summaryWindows.put(streamManager.streamID, (stream = new ConcurrentHashMap<>()));
        }
        stream.put(swid, window);
    }

    @Override
    public LandmarkWindow getLandmarkWindow(StreamManager streamManager, long lwid) throws RocksDBException {
        return landmarkWindows.get(streamManager.streamID).get(lwid);
    }

    @Override
    public void putLandmarkWindow(StreamManager streamManager, long lwid, LandmarkWindow window) throws RocksDBException {
        Map<Long, LandmarkWindow> stream = landmarkWindows.get(streamManager.streamID);
        if (stream == null) {
            landmarkWindows.put(streamManager.streamID, (stream = new ConcurrentHashMap<>()));
        }
        stream.put(lwid, window);
    }

    @Override
    public void close() throws RocksDBException {
        summaryWindows.clear();
    }
}
