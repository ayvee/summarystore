package com.samsung.sra.datastore.storage;

import com.samsung.sra.datastore.LandmarkWindow;
import com.samsung.sra.datastore.SummaryWindow;

import java.util.stream.Stream;

/**
 * Underlying key-value store holding all windows from all streams. Implements stream-independent window get/put logic.
 * Most of the code should not talk to BackingStore directly and should go through StreamWindowManager instead.
 */
public abstract class BackingStore implements AutoCloseable {
    abstract SummaryWindow getSummaryWindow(long streamID, long swid, SerDe serDe) throws BackingStoreException;

    /**
     * Optional. If using a backing store that does not override this method, must enable read indexes in SummaryStore
     * (which will be used to issue point queries for each window)
     */
    Stream<SummaryWindow> getSummaryWindowsOverlapping(long streamID, long t0, long t1, SerDe serDe)
            throws BackingStoreException {
        throw new UnsupportedOperationException("not implemented; please maintain a read index instead");
    }

    abstract void deleteSummaryWindow(long streamID, long swid, SerDe serDe) throws BackingStoreException;

    abstract void putSummaryWindow(long streamID, long swid, SerDe serDe, SummaryWindow window) throws BackingStoreException;

    /**
     * Optional. If using a backing store that does not override this method, must enable read indexes in SummaryStore
     */
    long getNumSummaryWindows(long streamID, SerDe serDe)
            throws BackingStoreException {
        throw new UnsupportedOperationException("not implemented; please maintain a read index instead");
    }

    abstract LandmarkWindow getLandmarkWindow(long streamID, long lwid, SerDe serDe) throws BackingStoreException;

    abstract void putLandmarkWindow(long streamID, long lwid, SerDe serDe, LandmarkWindow window) throws BackingStoreException;

    abstract public byte[] getAux(String key) throws BackingStoreException;

    abstract public void putAux(String key, byte[] value) throws BackingStoreException;

    /** Flush all entries for specified stream to disk */
    void flushToDisk(long streamID, SerDe serDe) throws BackingStoreException {}

    @Override
    abstract public void close() throws BackingStoreException;
}
