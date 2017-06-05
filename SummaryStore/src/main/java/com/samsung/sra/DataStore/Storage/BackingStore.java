package com.samsung.sra.DataStore.Storage;

import com.samsung.sra.DataStore.LandmarkWindow;
import com.samsung.sra.DataStore.SummaryWindow;

import java.util.stream.Stream;

/**
 * Underlying key-value store holding all windows from all streams. Most of the code should not talk to BackingStore
 * directly and should go through StreamWindowManager instead.
 */
public abstract class BackingStore implements AutoCloseable {
    abstract SummaryWindow getSummaryWindow(StreamWindowManager windowManager, long swid) throws BackingStoreException;

    abstract Stream<SummaryWindow> getSummaryWindowsOverlapping(StreamWindowManager windowManager, long t0, long t1) throws BackingStoreException;

    abstract SummaryWindow deleteSummaryWindow(StreamWindowManager windowManager, long swid) throws BackingStoreException;

    abstract void putSummaryWindow(StreamWindowManager windowManager, long swid, SummaryWindow window) throws BackingStoreException;

    abstract long getNumSummaryWindows(StreamWindowManager windowManager) throws BackingStoreException;

    abstract LandmarkWindow getLandmarkWindow(StreamWindowManager windowManager, long lwid) throws BackingStoreException;

    abstract void putLandmarkWindow(StreamWindowManager windowManager, long lwid, LandmarkWindow window) throws BackingStoreException;

    abstract void printWindowState(StreamWindowManager windowManager) throws BackingStoreException;

    /** Flush all entries for specified stream to disk */
    void flushToDisk(StreamWindowManager windowManager) throws BackingStoreException {}

    @Override
    abstract public void close() throws BackingStoreException;
}
