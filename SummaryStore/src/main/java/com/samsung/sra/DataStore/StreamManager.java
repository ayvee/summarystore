package com.samsung.sra.DataStore;

import com.google.protobuf.InvalidProtocolBufferException;
import com.samsung.sra.protocol.SummaryStore.ProtoSummaryWindow;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
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

    final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Read index, maps summaryWindow.tStart -> summaryWindow.ID. Used to answer queries
     */
    final TreeMap<Long, Long> summaryWindowIndex = new TreeMap<>();
    //transient BTreeMap<Long, Long> summaryWindowIndex;

    /**
     * WindowingMechanism object. Maintains write indexes internally, which SummaryStore
     * will persist to disk along with the rest of StreamManager
     */
    final WindowingMechanism windowingMechanism;

    private transient BackingStore backingStore;
    private transient ExecutorService executorService;

    void populateTransientFields(BackingStore backingStore, ExecutorService executorService) {
        this.backingStore = backingStore;
        this.executorService = executorService;
        windowingMechanism.populateTransientFields();
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
    }

    void append(long ts, Object[] value) throws RocksDBException, StreamException {
        if (ts <= stats.getTimeRangeEnd()) throw new StreamException("out-of-order insert in stream " + streamID +
                ": <ts, val> = <" + ts + ", " + value + ">, last arrival = " + stats.getTimeRangeEnd());
        stats.append(ts, value);
        windowingMechanism.append(this, ts, value);
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
        //BTreeMap<Long, Long> index = streamManager.summaryWindowIndex;
        Long sL = summaryWindowIndex.floorKey(t0); // first window with tStart <= t0
        Long sR = summaryWindowIndex.higherKey(t1); // first window with tStart > t1
        if (sR == null) {
            sR = summaryWindowIndex.lastKey() + 1;
        }
        //logger.debug("Overapproximated time range = [{}, {})", sL, sR);

        // Query on all windows with sL <= tStart < sR
        Stream<SummaryWindow> summaryWindows = summaryWindowIndex
                .subMap(sL, true, sR, false).values().stream()
                .map(swid -> {
            try {
                return backingStore.isColumnar()
                        ? backingStore.getSummaryWindow(this, swid, operatorNum)
                        : backingStore.getSummaryWindow(this, swid);
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        });
        try {
            Function<SummaryWindow, Object> retriever = backingStore.isColumnar()
                    ? b -> b.aggregates[operatorNum]
                    : b -> b.aggregates[0];
            return operators[operatorNum].query(stats, sL, sR - 1, summaryWindows, retriever, t0, t1, queryParams);
        } catch (RuntimeException e) {
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
