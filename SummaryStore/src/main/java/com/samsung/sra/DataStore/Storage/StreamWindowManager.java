package com.samsung.sra.DataStore.Storage;

import com.google.protobuf.InvalidProtocolBufferException;
import com.samsung.sra.DataStore.LandmarkWindow;
import com.samsung.sra.DataStore.StreamStatistics;
import com.samsung.sra.DataStore.SummaryWindow;
import com.samsung.sra.DataStore.WindowOperator;
import com.samsung.sra.protocol.SummaryStore.ProtoSummaryWindow;
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
    final long streamID;
    private final WindowOperator[] operators;

    /* Read indexes, map window.tStart -> window.ID. Used when answering queries */
    private final TreeSet<Long> summaryWindowStarts = new TreeSet<>();
    private final TreeSet<Long> landmarkWindowStarts = new TreeSet<>();
    //transient BTreeMap<Long, Long> summaryWindowIndex;

    public StreamWindowManager(long streamID, WindowOperator[] operators) {
        this.streamID = streamID;
        this.operators = operators;
    }

    public void populateTransientFields(BackingStore backingStore) {
        this.backingStore = backingStore;
    }

    public SummaryWindow createEmptySummaryWindow(long ts, long te, long cs, long ce, long prevTS, long nextTS) {
        return new SummaryWindow(operators, ts, te, cs, ce, prevTS, nextTS);
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
        if (summaryWindowStarts.isEmpty()) {
            return Stream.empty();
        }
        Long l = summaryWindowStarts.floor(t0); // first window with tStart <= t0
        Long r = summaryWindowStarts.higher(t1); // first window with tStart > t1
        if (r == null) {
            r = summaryWindowStarts.last() + 1;
        }
        //logger.debug("Overapproximated time range = [{}, {})", l, r);
        // Query on all windows with l <= tStart < r
        return summaryWindowStarts
                .subSet(l, true, r, false).stream()
                .map(swid -> {
                    try {
                        return getSummaryWindow(swid);
                    } catch (BackingStoreException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    public SummaryWindow deleteSummaryWindow(long swid) throws BackingStoreException {
        SummaryWindow window = backingStore.deleteSummaryWindow(this, swid);
        assert window.ts == swid;
        summaryWindowStarts.remove(window.ts);
        return window;
    }

    public void putSummaryWindow(SummaryWindow window) throws BackingStoreException {
        summaryWindowStarts.add(window.ts);
        backingStore.putSummaryWindow(this, window.ts, window);
    }

    public long getNumSummaryWindows() {
        return summaryWindowStarts.size();
    }

    byte[] serializeSummaryWindow(SummaryWindow window) {
        assert window != null;
        ProtoSummaryWindow.Builder protoWindow;
        try {
            protoWindow = ProtoSummaryWindow.newBuilder()
                    .setTs(window.ts)
                    .setTe(window.te)
                    .setCs(window.cs)
                    .setCe(window.ce)
                    .setPrevTS(window.prevTS)
                    .setNextTS(window.nextTS);
        } catch (Exception e) {
            logger.error("Exception in serializing window", e);
            throw new RuntimeException(e);
        }

        for (int op = 0; op < operators.length; ++op) {
            try {
                assert window.aggregates[op] != null;
                protoWindow.addOperator(operators[op].protofy(window.aggregates[op]));
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        return protoWindow.build().toByteArray();
    }

    SummaryWindow deserializeSummaryWindow(byte[] bytes) {
        ProtoSummaryWindow protoSummaryWindow;
        try {
            protoSummaryWindow = ProtoSummaryWindow.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        SummaryWindow window = new SummaryWindow();
        window.ts = protoSummaryWindow.getTs();
        window.te = protoSummaryWindow.getTe();
        window.cs = protoSummaryWindow.getCs();
        window.ce = protoSummaryWindow.getCe();
        window.prevTS = protoSummaryWindow.getPrevTS();
        window.nextTS = protoSummaryWindow.getNextTS();

        assert protoSummaryWindow.getOperatorCount() == operators.length;
        window.aggregates = new Object[operators.length];
        for (int op = 0; op < operators.length; ++op) {
            window.aggregates[op] = operators[op].deprotofy(protoSummaryWindow.getOperator(op));
        }

        return window;
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

    public void printWindows(boolean printPerWindowState, StreamStatistics stats) throws BackingStoreException {
        System.out.println(String.format("Stream %d with %d elements in %d summary windows and %d landmark windows",
                streamID, stats.getNumValues(), getNumSummaryWindows(), getNumLandmarkWindows()));
        if (printPerWindowState) {
            for (long swid : summaryWindowStarts) {
                System.out.println("\t" + getSummaryWindow(swid));
            }
            for (long lwid : landmarkWindowStarts) {
                System.out.println("\t" + getLandmarkWindow(lwid));
            }
        }
    }
}
