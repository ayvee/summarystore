package com.samsung.sra.DataStore.Storage;

import com.samsung.sra.DataStore.LandmarkWindow;
import com.samsung.sra.DataStore.SummaryWindow;
import com.samsung.sra.DataStore.WindowOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * Handles all operations on the windows in one stream, including creating/merging/inserting into window objects and
 * getting window objects into/out of the backing store. Wraps BackingStore: most code outside this package should
 * talk to StreamWindowManager and not to BackingStore directly.
 */
public class StreamWindowManager implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(StreamWindowManager.class);
    public static final Object LANDMARK_SENTINEL = new Object(); // sentinel used when handling append

    private transient BackingStore backingStore;
    public final long streamID;
    private final WindowOperator[] operators;
    private final SerDe serDe;

    private final SummaryWindowIndex swIndex = new SummaryWindowIndex();
    private final TreeSet<Long> landmarkWindowStarts = new TreeSet<>(); // TODO: create thought out LandmarkWindowIndex

    public StreamWindowManager(long streamID, WindowOperator[] operators) {
        this.streamID = streamID;
        this.operators = operators;
        this.serDe = new SerDe(operators);
    }

    public void populateTransientFields(BackingStore backingStore) {
        this.backingStore = backingStore;
        backingStore.setSerDe(streamID, serDe);
    }

    public SummaryWindow createEmptySummaryWindow(long ts, long te, long cs, long ce) {
        return new SummaryWindow(operators, ts, te, cs, ce);
    }

    public void insertIntoSummaryWindow(SummaryWindow window, long ts, Object value) {
        assert window.ts <= ts && (window.te == -1 || ts <= window.te)
                && operators.length == window.aggregates.length;
        if (value == LANDMARK_SENTINEL) {
            // value is actually going into landmark bucket, do nothing here. We only processed it this far so that the
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
        return backingStore.getSummaryWindow(this, swid);
    }

    /** Get all summary windows overlapping [t0, t1] */
    public Stream<SummaryWindow> getSummaryWindowsOverlapping(long t0, long t1) throws BackingStoreException {
        return swIndex.getOverlappingSWIDs(t0, t1).map(swid -> {
            try {
                return backingStore.getSummaryWindow(this, swid);
            } catch (BackingStoreException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public SummaryWindow deleteSummaryWindow(long swid) throws BackingStoreException {
        swIndex.remove(swid);
        return backingStore.deleteSummaryWindow(this, swid);
    }

    public void putSummaryWindow(SummaryWindow window) throws BackingStoreException {
        swIndex.put(window);
        backingStore.putSummaryWindow(this, window.ts, window);
    }

    public long getNumSummaryWindows() throws BackingStoreException {
        return swIndex.getNumWindows();
    }

    public LandmarkWindow getLandmarkWindow(long lwid) throws BackingStoreException {
        return backingStore.getLandmarkWindow(this, lwid);
    }

    /** Get all landmark windows overlapping [t0, t1] */
    public Stream<LandmarkWindow> getLandmarkWindowsOverlapping(long t0, long t1) throws BackingStoreException {
        if (landmarkWindowStarts.isEmpty()) { // no landmarks
            return Stream.empty();
        }
        Long l = landmarkWindowStarts.floor(t0); // first window with tStart <= t0
        Long r = landmarkWindowStarts.higher(t1); // first window with tStart > t1
        if (l == null) {
            l = landmarkWindowStarts.first();
        }
        if (r == null) {
            r = landmarkWindowStarts.last() + 1;
        }
        //logger.debug("Overapproximated time range = [{}, {})", l, r);
        // Query on all windows with l <= tStart < r
        return landmarkWindowStarts
                .subSet(l, true, r, false).stream()
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
        landmarkWindowStarts.add(window.ts);
        backingStore.putLandmarkWindow(this, window.ts, window);
    }

    public long getNumLandmarkWindows() {
        return landmarkWindowStarts.size();
    }

    public void printWindows() throws BackingStoreException {
        swIndex.getOverlappingSWIDs(0, Long.MAX_VALUE).forEach(swid -> {
            try {
                System.out.println("\t" + backingStore.getSummaryWindow(this, swid));
            } catch (BackingStoreException e) {
                throw new RuntimeException(e);
            }
        });
        //backingStore.printWindowState(this);
    }

    public void flushToDisk() throws BackingStoreException {
        backingStore.flushToDisk(this);
    }
}
