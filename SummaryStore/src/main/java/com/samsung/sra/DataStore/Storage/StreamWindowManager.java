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
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * Handles all operations on the windows in one stream, including creating/merging/inserting into window objects and
 * getting window objects into/out of the backing store. Wraps BackingStore: most code outside this package should
 * talk to StreamWindowManager and not to BackingStore directly.
 */
public class StreamWindowManager implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(StreamWindowManager.class);
    public static final Object[] LANDMARK_SENTINEL = {}; // sentinel used when handling append

    private transient BackingStore backingStore;
    final long streamID;
    private final WindowOperator[] operators;

    /* Read indexes, map window.tStart -> window.ID. Used when answering queries */
    private final TreeMap<Long, Long> summaryWindowIndex = new TreeMap<>();
    private final TreeMap<Long, Long> landmarkWindowIndex = new TreeMap<>();
    //transient BTreeMap<Long, Long> summaryWindowIndex;

    public StreamWindowManager(long streamID, WindowOperator[] operators) {
        this.streamID = streamID;
        this.operators = operators;
    }

    public void populateTransientFields(BackingStore backingStore) {
        this.backingStore = backingStore;
    }

    public SummaryWindow createEmptySummaryWindow(
            long prevSWID, long thisSWID, long nextSWID,
            long tStart, long tEnd, long cStart, long cEnd) {
        return new SummaryWindow(operators, prevSWID, thisSWID, nextSWID, tStart, tEnd, cStart, cEnd);
    }

    public void insertIntoSummaryWindow(SummaryWindow window, long ts, Object[] value) {
        assert window.tStart <= ts && (window.tEnd == -1 || ts <= window.tEnd)
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
        windows[0].cEnd = windows[windows.length - 1].cEnd;
        windows[0].tEnd = windows[windows.length - 1].tEnd;

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
        Long l = summaryWindowIndex.floorKey(t0); // first window with tStart <= t0
        Long r = summaryWindowIndex.higherKey(t1); // first window with tStart > t1
        if (r == null) {
            r = summaryWindowIndex.lastKey() + 1;
        }
        //logger.debug("Overapproximated time range = [{}, {})", l, r);
        // Query on all windows with l <= tStart < r
        return summaryWindowIndex
                .subMap(l, true, r, false).values().stream()
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
        summaryWindowIndex.remove(window.tStart);
        return window;
    }

    public void putSummaryWindow(long swid, SummaryWindow window) throws BackingStoreException {
        summaryWindowIndex.put(window.tStart, swid);
        backingStore.putSummaryWindow(this, swid, window);
    }

    public long getNumSummaryWindows() {
        return summaryWindowIndex.size();
    }


    byte[] serializeSummaryWindow(SummaryWindow window) {
        assert window != null;
        ProtoSummaryWindow.Builder protoWindow;
        try {
            protoWindow = ProtoSummaryWindow.newBuilder().
                    setThisSWID(window.thisSWID).
                    setNextSWID(window.nextSWID).
                    setPrevSWID(window.prevSWID).
                    setTStart(window.tStart).setTEnd(window.tEnd).
                    setCStart(window.cStart).setCEnd(window.cEnd);
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
        window.thisSWID = protoSummaryWindow.getThisSWID();
        window.prevSWID = protoSummaryWindow.getPrevSWID();
        window.nextSWID = protoSummaryWindow.getNextSWID();
        window.tStart = protoSummaryWindow.getTStart();
        window.tEnd = protoSummaryWindow.getTEnd();
        window.cStart = protoSummaryWindow.getCStart();
        window.cEnd = protoSummaryWindow.getCEnd();

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
        if (landmarkWindowIndex.isEmpty()) { // no landmarks
            return Stream.empty();
        }
        Long l = landmarkWindowIndex.floorKey(t0); // first window with tStart <= t0
        Long r = landmarkWindowIndex.higherKey(t1); // first window with tStart > t1
        if (l == null) {
            l = landmarkWindowIndex.firstKey();
        }
        if (r == null) {
            r = landmarkWindowIndex.lastKey() + 1;
        }
        //logger.debug("Overapproximated time range = [{}, {})", l, r);
        // Query on all windows with l <= tStart < r
        return landmarkWindowIndex
                .subMap(l, true, r, false).values().stream()
                .map(lwid -> {
                    try {
                        return getLandmarkWindow(lwid);
                    } catch (BackingStoreException e) {
                        throw new RuntimeException(e);
                    }
                })
                .filter(w -> w.tEnd >= t0); // filter needed because very first window may not overlap [t0, t1]
    }

    public void putLandmarkWindow(long lwid, LandmarkWindow window) throws BackingStoreException {
        landmarkWindowIndex.put(window.tStart, lwid);
        backingStore.putLandmarkWindow(this, lwid, window);
    }

    public long getNumLandmarkWindows() {
        return landmarkWindowIndex.size();
    }

    public void printWindows(boolean printPerWindowState, StreamStatistics stats) throws BackingStoreException {
        System.out.println(String.format("Stream %d with %d elements in %d summary windows and %d landmark windows",
                streamID, stats.getNumValues(), getNumSummaryWindows(), getNumLandmarkWindows()));
        if (printPerWindowState) {
            for (long swid : summaryWindowIndex.values()) {
                System.out.println("\t" + getSummaryWindow(swid));
            }
            for (long lwid : landmarkWindowIndex.values()) {
                System.out.println("\t" + getLandmarkWindow(lwid));
            }
        }
    }
}
