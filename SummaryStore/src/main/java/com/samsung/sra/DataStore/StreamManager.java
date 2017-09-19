package com.samsung.sra.DataStore;

import com.google.protobuf.InvalidProtocolBufferException;
import com.samsung.sra.protocol.Common.OpType;
import com.samsung.sra.protocol.SummaryStore.ProtoSummaryWindow;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Stream;


/**
 * Handles all operations on a particular stream, both the southbound SummaryWindow get, create
 * and merge operations initiated by WBMH as well as the northbound append() and query()
 * operations initiated by clients via SummaryStore. Stores aggregate per-stream information
 * as well as in-memory indexes. The windows themselves are not stored directly here, they
 * go into the underlying backingStore.
 * <p>
 * StreamManagers do not handle synchronization, callers are expected to manage the lock
 * object here.
 */
class StreamManager implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(StreamManager.class);

    final long streamID;
    final StreamStatistics stats;
    private final WindowOperator[] operators;

    private long tLastAppend = -1, tLastLandmarkStart = -1, tLastLandmarkEnd = -1;
    private long activeLWID = -1; // id of active landmark window
    private long nextLWID = 0; // id of next landmark window to be created

    private final Object[] LANDMARK_VALUE = {}; // sentinel used when handling append

    final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Read indexes, map window.tStart -> window.ID. Used to answer queries
     */
    final TreeMap<Long, Long> summaryWindowIndex = new TreeMap<>();
    final TreeMap<Long, Long> landmarkWindowIndex = new TreeMap<>();
    //transient BTreeMap<Long, Long> summaryWindowIndex;

    /**
     * WindowingMechanism object. Maintains write indexes internally, which SummaryStore
     * will persist to disk along with the rest of StreamManager
     */
    final WindowingMechanism windowingMechanism;

    private transient BackingStore backingStore;

    private transient ExecutorService executorService;

    private final HashMap<OpType, Integer> opSequenceMap = new HashMap<>();


    void populateTransientFields(BackingStore backingStore, ExecutorService executorService) {
        this.backingStore = backingStore;
        this.executorService = executorService;
        windowingMechanism.populateTransientFields();
    }

    private void populateOpSequenceMap(WindowOperator... operators) {
        int seq=0;
        for(WindowOperator operator:operators) {
            if(opSequenceMap.containsKey(operator.getOpType())) {
                logger.error("OpSequenceMap already has operator for " + operator.getOpType().toString()
                + "\nFIXME: currently store does not support multiple ops for same query type");
            } else {
                opSequenceMap.put(operator.getOpType(), seq++);
            }
        }
    }

    public int getSequenceForOp(OpType opType) {
        if(opSequenceMap.containsKey(opType)) {
            return opSequenceMap.get(opType);
        } else {
            logger.error("No operator found for Type: " + opType.toString());
            return -1;
        }
    }

    ExecutorService getExecutorService() {
        return executorService;
    }

    StreamManager(BackingStore backingStore, ExecutorService executorService,
                  long streamID,
                  WindowingMechanism windowingMechanism, WindowOperator... operators) {
        this.streamID = streamID;
        this.backingStore = backingStore;
        this.executorService = executorService;
        this.windowingMechanism = windowingMechanism;
        this.operators = operators;
        this.stats = new StreamStatistics();

        populateOpSequenceMap(operators);
    }

    void append(long ts, Object[] value) throws RocksDBException, StreamException {
        if (ts <= tLastAppend || ts < tLastLandmarkStart || ts <= tLastLandmarkEnd) {
            throw new StreamException("out-of-order insert in stream " + streamID +
                    ": <ts, val> = <" + ts + ", " + value + ">, last arrival = " + stats.getTimeRangeEnd());
        }
        tLastAppend = ts;
        stats.append(ts, value);
        if (activeLWID == -1) {
            // insert into decayed window sequence
            windowingMechanism.append(this, ts, value);
        } else {
            // update decayed windowing, aging it by one position, but don't actually insert value into decayed window;
            // see how LANDMARK_VALUE is handled in insertIntoSummaryWindow below
            windowingMechanism.append(this, ts, LANDMARK_VALUE);
            LandmarkWindow window = backingStore.getLandmarkWindow(this, activeLWID);
            window.append(ts, value);
            backingStore.putLandmarkWindow(this, activeLWID, window);
        }
    }

    void startLandmark(long ts) throws LandmarkException, RocksDBException {
        if (ts <= tLastAppend || ts < tLastLandmarkStart || ts <= tLastLandmarkEnd) {
            throw new LandmarkException("attempting to retroactively start landmark");
        }
        if (activeLWID != -1) {
            return;
        }
        tLastLandmarkStart = ts;
        activeLWID = nextLWID++;
        backingStore.putLandmarkWindow(this, activeLWID, new LandmarkWindow(activeLWID, ts));
        landmarkWindowIndex.put(ts, activeLWID);
    }

    void endLandmark(long ts) throws LandmarkException, RocksDBException {
        if (ts < tLastAppend || ts < tLastLandmarkStart || ts <= tLastLandmarkEnd) {
            throw new LandmarkException("attempting to retroactively end landmark");
        }
        tLastLandmarkEnd = ts;
        LandmarkWindow window = backingStore.getLandmarkWindow(this, activeLWID);
        window.close(ts);
        backingStore.putLandmarkWindow(this, activeLWID, window);
        activeLWID = -1;
    }

    Object query(int operatorNum, long t0, long t1, Object[] queryParams) throws RocksDBException {
        long T0 = stats.getTimeRangeStart(), T1 = stats.getTimeRangeEnd();
        if (t0 > T1) {
            return operators[operatorNum].getEmptyQueryResult();
        } else if (t0 < T0) {
            t0 = T0;
        }
        if (t1 > T1) {
            t1 = T1;
        }

        Stream<SummaryWindow> summaryWindows;
        {
            Long l = summaryWindowIndex.floorKey(t0); // first window with tStart <= t0
            Long r = summaryWindowIndex.higherKey(t1); // first window with tStart > t1

            if (!summaryWindowIndex.isEmpty()) {
                    try {
                        r = summaryWindowIndex.lastKey() + 1;
                        l = summaryWindowIndex.firstKey();
                    } catch (NoSuchElementException e) {
                        e.printStackTrace();
                        //return operators[operatorNum].getEmptyQueryResult();
                    }
            } else {
                // FIXME: summarywindows is null sometimes. when the data is entirely in the buffer
                logger.error("Empty summaryWindowIndex; returning empty result");
                return operators[operatorNum].getEmptyQueryResult();
            }

            //logger.debug("Overapproximated time range = [{}, {})", l, r);
            // Query on all windows with l <= tStart < r
            summaryWindows = summaryWindowIndex
                    .subMap(l, true, r, false).values().stream()
                    .map(swid -> {
                        try {
                            return backingStore.isColumnar()
                                    ? backingStore.getSummaryWindow(this, swid, operatorNum)
                                    : backingStore.getSummaryWindow(this, swid);
                        } catch (RocksDBException e) {
                            throw new RuntimeException(e);
                        }
                    });




        }

        Stream<LandmarkWindow> landmarkWindows;
        if (landmarkWindowIndex.isEmpty()) { // no landmarks
            landmarkWindows = Stream.empty();
        } else {
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
            long _t0 = t0; // hack, needed because of a limitation in Java 8's lambda function handling
            landmarkWindows = landmarkWindowIndex
                    .subMap(l, true, r, false).values().stream()
                    .map(lwid -> {
                        try {
                            return backingStore.getLandmarkWindow(this, lwid);
                        } catch (RocksDBException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .filter(w -> w.tEnd >= _t0); // filter needed because very first window may not overlap [t0, t1]
        }

        try {
            Function<SummaryWindow, Object> retriever = !backingStore.isColumnar()
                    ? b -> b.aggregates[operatorNum]
                    : b -> b.aggregates[0];
            // FIXME; why empty, NULL rocksValue?
            if(summaryWindows == null) {
                logger.error("NULL/empty SummaryWindows prior to query; returning empty result");
                return operators[operatorNum].getEmptyQueryResult();
            } else {
                return operators[operatorNum].query(stats, summaryWindows, retriever, landmarkWindows, t0, t1, queryParams);
            }
        } catch (RuntimeException e) {

            logger.error("Exception in Stream Manager querying");
            if (e.getCause() instanceof RocksDBException) {
                throw (RocksDBException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    void insertIntoSummaryWindow(SummaryWindow window, long ts, Object[] value) {
        assert window.tStart <= ts && (window.tEnd == -1 || ts <= window.tEnd)
                && operators.length == window.aggregates.length;
        if (value == LANDMARK_VALUE) {
            // value is actually going into landmark bucket, do nothing here. We only processed it this far so that the
            // decayed windowing would be updated by one position
            return;
        }
        for (int i = 0; i < operators.length; ++i) {
            window.aggregates[i] = operators[i].insert(window.aggregates[i], ts, value);
        }
    }

    /**
     * Replace windows[0] with union(windows)
     */
    void mergeSummaryWindows(SummaryWindow... windows) {
        if (windows.length == 0) return;
        windows[0].cEnd = windows[windows.length - 1].cEnd;
        windows[0].tEnd = windows[windows.length - 1].tEnd;

        for (int opNum = 0; opNum < operators.length; ++opNum) {
            final int i = opNum; // work around Java dumbness re stream.map arguments
            windows[0].aggregates[i] = operators[i].merge(Stream.of(windows).map(b -> b.aggregates[i]));
        }
    }

    SummaryWindow createEmptySummaryWindow(
            long prevSWID, long thisSWID, long nextSWID,
            long tStart, long tEnd, long cStart, long cEnd) {
        return new SummaryWindow(operators, prevSWID, thisSWID, nextSWID, tStart, tEnd, cStart, cEnd);
    }

    SummaryWindow getSummaryWindow(long swid) throws RocksDBException {
        logger.info("getWindow: " + swid);
        return backingStore.getSummaryWindow(this, swid);
    }

    SummaryWindow deleteSummaryWindow(long swid) throws RocksDBException {
        SummaryWindow window = backingStore.deleteSummaryWindow(this, swid);
        summaryWindowIndex.remove(window.tStart);
        return window;
    }

    void putSummaryWindow(long swid, SummaryWindow window) throws RocksDBException {
        summaryWindowIndex.put(window.tStart, swid);
        backingStore.putSummaryWindow(this, swid, window);
    }


    byte[] serializeSummaryWindow(SummaryWindow window) {
        if (window == null) {
            logger.error("NULL SummaryWindow about to be serialized");
        }

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
}
