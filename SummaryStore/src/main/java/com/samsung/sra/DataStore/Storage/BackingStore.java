package com.samsung.sra.DataStore.Storage;

import com.samsung.sra.DataStore.LandmarkWindow;
import com.samsung.sra.DataStore.SummaryWindow;

/**
 * Underlying key-value store holding all windows from all streams. Implements stream-independent window get/put logic.
 * Most of the code should not talk to BackingStore directly and should go through StreamWindowManager instead.
 */
public abstract class BackingStore implements AutoCloseable {
    abstract SummaryWindow getSummaryWindow(long streamID, long swid, SerDe serDe) throws BackingStoreException;

    abstract SummaryWindow deleteSummaryWindow(long streamID, long swid, SerDe serDe) throws BackingStoreException;

    abstract void putSummaryWindow(long streamID, long swid, SerDe serDe, SummaryWindow window) throws BackingStoreException;

    abstract LandmarkWindow getLandmarkWindow(long streamID, long lwid, SerDe serDe) throws BackingStoreException;

    abstract void putLandmarkWindow(long streamID, long lwid, SerDe serDe, LandmarkWindow window) throws BackingStoreException;

    /** Flush all entries for specified stream to disk */
    void flushToDisk(long streamID, SerDe serDe) throws BackingStoreException {}

    @Override
    abstract public void close() throws BackingStoreException;
}
