package com.samsung.sra.DataStore;

import org.rocksdb.RocksDBException;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class MainMemoryBackingStore implements BackingStore {
    private Map<Long, Map<Long, SummaryWindow>> summaryWindows = new HashMap<>();

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
            summaryWindows.put(streamManager.streamID, (stream = new HashMap<>()));
        }
        stream.put(swid, window);
    }

    private Serializable indexes = null;

    @Override
    public Serializable getMetadata() throws RocksDBException {
        return indexes;
    }

    @Override
    public void putMetadata(Serializable indexes) throws RocksDBException {
        this.indexes = indexes;
    }

    @Override
    public void close() throws RocksDBException {
        summaryWindows.clear();
        indexes = null;
    }
}
