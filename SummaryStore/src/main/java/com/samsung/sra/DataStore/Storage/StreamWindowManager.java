package com.samsung.sra.DataStore.Storage;

import com.samsung.sra.DataStore.LandmarkWindow;
import com.samsung.sra.DataStore.SummaryWindow;
import com.samsung.sra.DataStore.WindowOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Handles all operations on the windows in one stream, including creating/merging/inserting into window objects and
 * getting window objects into/out of the backing store. Acts as a proxy to BackingStore: most code outside this package
 * should talk to StreamWindowManager and not to BackingStore directly.
 */
public class StreamWindowManager implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(StreamWindowManager.class);
    // FIXME: what if user actually wants to insert Long.MIN_VALUE?
    public static final Object LANDMARK_SENTINEL = Long.MIN_VALUE; // sentinel used when handling append

    private transient BackingStore backingStore;
    public final long streamID;
    private final WindowOperator[] operators;
    private final SerDe serde;

    private final QueryIndex summaryIndex, landmarkIndex;

    public StreamWindowManager(long streamID, WindowOperator[] operators, boolean keepReadIndex) {
        this.streamID = streamID;
        this.operators = operators;
        this.serde = new SerDe(operators);
        this.summaryIndex = keepReadIndex ? new QueryIndex() : null;
        // FIXME: should handle landmarks the same as summary windows
        this.landmarkIndex = new QueryIndex();
    }

    public void populateTransientFields(BackingStore backingStore) {
        this.backingStore = backingStore;
    }

    public SummaryWindow createEmptySummaryWindow(long ts, long te, long cs, long ce) {
        return new SummaryWindow(operators, ts, te, cs, ce);
    }

    public void insertIntoSummaryWindow(SummaryWindow window, long ts, Object value) {
        assert window.ts <= ts && (window.te == -1 || ts <= window.te)
                && operators.length == window.aggregates.length;
        // FIXME: if condition works for both long and Object WBMH buffers, but at the cost that it fails when user
        // tries to insert Long.MIN_VALUE even in the Object case
        if (LANDMARK_SENTINEL == value || LANDMARK_SENTINEL.equals(value)) {
            // value is actually going into landmark window, do nothing here. We only processed it this far so that the
            // decayed windowing would be updated by one position
            return;
        }
        for (int i = 0; i < operators.length; ++i) {
            window.aggregates[i] = operators[i].insert(window.aggregates[i], ts, value);
        }
    }

    /** Replace windows[0] with union(windows) */
    public void mergeSummaryWindows(SummaryWindow... windows) {
        if (windows.length == 0) return;
        windows[0].ce = windows[windows.length - 1].ce;
        windows[0].te = windows[windows.length - 1].te;

        for (int opNum = 0; opNum < operators.length; ++opNum) {
            final int i = opNum; // work around Java dumbness re stream.map arguments
            windows[0].aggregates[i] = operators[i].merge(Stream.of(windows).map(b -> b.aggregates[i]));
        }
    }

    public SummaryWindow getSummaryWindow(long swid) throws BackingStoreException {
        return backingStore.getSummaryWindow(streamID, swid, serde);
    }

    /** Get all summary windows overlapping [t0, t1] */
    public Stream<SummaryWindow> getSummaryWindowsOverlapping(long t0, long t1) throws BackingStoreException {
        return summaryIndex == null
                ? backingStore.getSummaryWindowsOverlapping(streamID, t0, t1, serde)
                : summaryIndex
                .getOverlappingWindowIDs(t0, t1)
                .map(swid -> {
                    try {
                        return backingStore.getSummaryWindow(streamID, swid, serde);
                    } catch (BackingStoreException e) {
                        throw new RuntimeException(e);
                    }
                })
                .filter(w -> w.te >= t0); // filter needed because very first window may not overlap [t0, t1]
    }

    public void deleteSummaryWindow(long swid) throws BackingStoreException {
        if (summaryIndex != null) summaryIndex.remove(swid);
        backingStore.deleteSummaryWindow(streamID, swid, serde);
    }

    public void putSummaryWindow(SummaryWindow window) throws BackingStoreException {
        if (summaryIndex != null) summaryIndex.add(window.ts);
        backingStore.putSummaryWindow(streamID, window.ts, serde, window);
    }

    public long getNumSummaryWindows() throws BackingStoreException {
        return summaryIndex == null
                ? backingStore.getNumSummaryWindows(streamID, serde)
                : summaryIndex.getNumWindows();
    }

    public LandmarkWindow getLandmarkWindow(long lwid) throws BackingStoreException {
        return backingStore.getLandmarkWindow(streamID, lwid, serde);
    }

    /** Get all landmark windows overlapping [t0, t1] */
    public Stream<LandmarkWindow> getLandmarkWindowsOverlapping(long t0, long t1) throws BackingStoreException {
        return landmarkIndex.getOverlappingWindowIDs(t0, t1)
                .map(lwid -> {
                    try {
                        return getLandmarkWindow(lwid);
                    } catch (BackingStoreException e) {
                        throw new RuntimeException(e);
                    }
                })
                .filter(w -> w.te >= t0); // filter needed because very first window may not overlap [t0, t1]
    }

    public void putLandmarkWindow(LandmarkWindow window) throws BackingStoreException {
        landmarkIndex.add(window.ts);
        backingStore.putLandmarkWindow(streamID, window.ts, serde, window);
    }

    public long getNumLandmarkWindows() {
        return landmarkIndex.getNumWindows();
    }

    public void printWindows() throws BackingStoreException {
        //backingStore.printWindowState(this);
        System.out.printf("Stream %d with %d summary windows and %d landmark windows\n", streamID,
                getNumSummaryWindows(), getNumLandmarkWindows());
        Consumer<Object> printTabbed = o -> System.out.println("\t" + o);
        getSummaryWindowsOverlapping(0, Long.MAX_VALUE - 10).forEach(printTabbed);
        getLandmarkWindowsOverlapping(0, Long.MAX_VALUE - 10).forEach(printTabbed);
    }

    public void flushToDisk() throws BackingStoreException {
        backingStore.flushToDisk(streamID, serde);
    }
}
