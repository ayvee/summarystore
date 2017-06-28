package com.samsung.sra.DataStore;

import com.samsung.sra.DataStore.Ingest.CountBasedWBMH;
import com.samsung.sra.DataStore.Storage.BackingStore;
import com.samsung.sra.DataStore.Storage.BackingStoreException;
import com.samsung.sra.DataStore.Storage.StreamWindowManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;


/** One Summary Store stream. This class has the outermost level of the logic for all major API calls. */
class Stream implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(Stream.class);

    final long streamID;
    private final WindowOperator[] operators;

    final StreamStatistics stats;
    private long tLastAppend = -1, tLastLandmarkStart = -1, tLastLandmarkEnd = -1;
    private boolean isLandmarkActive = false;

    /**
     * Lock used to serialize external write actions (append, start/end landmark, flush, close). May be turned on/off
     * via a boolean flag to SummaryStore.registerStream */
    private final Lock extLock;
    private final boolean synchronizeWrites;

    /** Must be loaded to handle any reads/writes */
    transient StreamWindowManager windowManager;
    /** Needed to handle writes, but can be unloaded in read-only mode. Maintains write indexes internally */
    transient CountBasedWBMH wbmh;

    void populateTransientFields(BackingStore backingStore) {
        if (windowManager != null) windowManager.populateTransientFields(backingStore);
        if (wbmh != null) wbmh.populateTransientFields(windowManager);
    }

    Stream(long streamID, boolean synchronizeWrites, CountBasedWBMH wbmh, WindowOperator[] operators, boolean keepReadIndex) {
        this.streamID = streamID;
        this.synchronizeWrites = synchronizeWrites;
        this.extLock = synchronizeWrites ? new ReentrantLock() : null;
        this.operators = operators;
        this.wbmh = wbmh;
        windowManager = new StreamWindowManager(streamID, operators, keepReadIndex);
        stats = new StreamStatistics();
    }

    void append(long ts, Object value) throws BackingStoreException, StreamException {
        if (synchronizeWrites) extLock.lock();
        try {
            if (ts <= tLastAppend || ts < tLastLandmarkStart || ts <= tLastLandmarkEnd) {
                throw new StreamException(String.format("out-of-order insert in stream %d: ts = %d", streamID, ts));
            }
            tLastAppend = ts;
            stats.append(ts, value);
            if (!isLandmarkActive) {
                // insert into decayed window sequence
                wbmh.append(ts, value);
            } else {
                // update decayed windowing, aging it by one position, but don't actually insert value into decayed window;
                // see how LANDMARK_SENTINEL is handled in StreamWindowManager.insertIntoSummaryWindow
                wbmh.append(ts, StreamWindowManager.LANDMARK_SENTINEL);
                LandmarkWindow window = windowManager.getLandmarkWindow(tLastLandmarkStart);
                window.append(ts, value);
                windowManager.putLandmarkWindow(window);
            }
        } finally {
            if (synchronizeWrites) extLock.unlock();
        }
    }

    void startLandmark(long ts) throws StreamException, BackingStoreException {
        if (synchronizeWrites) extLock.lock();
        try {
            if (ts <= tLastAppend || ts < tLastLandmarkStart || ts <= tLastLandmarkEnd) {
                throw new StreamException("attempting to retroactively start landmark");
            }
            if (isLandmarkActive) {
                return;
            }
            tLastLandmarkStart = ts;
            isLandmarkActive = true;
            windowManager.putLandmarkWindow(new LandmarkWindow(ts));
        } finally {
            if (synchronizeWrites) extLock.unlock();
        }
    }

    void endLandmark(long ts) throws StreamException, BackingStoreException {
        if (synchronizeWrites) extLock.lock();
        try {
            if (ts < tLastAppend || ts < tLastLandmarkStart || ts <= tLastLandmarkEnd) {
                throw new StreamException("attempting to retroactively end landmark");
            }
            tLastLandmarkEnd = ts;
            LandmarkWindow window = windowManager.getLandmarkWindow(tLastLandmarkStart);
            window.close(ts);
            windowManager.putLandmarkWindow(window);
            isLandmarkActive = false;
        } finally {
            if (synchronizeWrites) extLock.unlock();
        }
    }

    Object query(int operatorNum, long t0, long t1, Object[] queryParams) throws BackingStoreException {
        long T0 = stats.getTimeRangeStart(), T1 = stats.getTimeRangeEnd();
        if (t0 > T1 || t1 < T0) { // [T0, T1] does not overlap [t0, t1]
            return operators[operatorNum].getEmptyQueryResult();
        } else {
            t0 = Math.max(t0, T0);
            t1 = Math.min(t1, T1);
        }

        java.util.stream.Stream summaryWindows = windowManager.getSummaryWindowsOverlapping(t0, t1);
        java.util.stream.Stream landmarkWindows = windowManager.getLandmarkWindowsOverlapping(t0, t1);
        Function<SummaryWindow, Object> summaryRetriever = b -> b.aggregates[operatorNum];
        try {
            return operators[operatorNum].query(
                    stats, summaryWindows, summaryRetriever, landmarkWindows, t0, t1, queryParams);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof BackingStoreException) {
                throw (BackingStoreException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    long getNumSummaryWindows() throws BackingStoreException {
        return windowManager.getNumSummaryWindows();
    }

    long getNumLandmarkWindows() {
        return windowManager.getNumLandmarkWindows();
    }

    void printWindows() throws BackingStoreException {
        windowManager.printWindows();
    }

    void flush() throws BackingStoreException {
        if (synchronizeWrites) extLock.lock();
        try {
            wbmh.flush();
        } finally {
            if (synchronizeWrites) extLock.unlock();
        }
    }

    void close() throws BackingStoreException {
        if (synchronizeWrites) extLock.lock(); // block all new writes
        wbmh.close();
        windowManager.flushToDisk();
    }
}
