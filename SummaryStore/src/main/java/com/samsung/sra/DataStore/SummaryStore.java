package com.samsung.sra.DataStore;

import com.samsung.sra.DataStore.Aggregates.CMSOperator;
import com.samsung.sra.DataStore.Aggregates.MaxOperator;
import com.samsung.sra.DataStore.Aggregates.SimpleCountOperator;
import com.samsung.sra.protocol.Common.OpType;
import org.rocksdb.RocksDBException;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Time-decayed aggregate storage
 */
public class SummaryStore implements AutoCloseable {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SummaryStore.class);

    private final BackingStore backingStore;

    private final ExecutorService executorService;

    private final ConcurrentHashMap<Long, StreamManager> streamManagers;

    private void persistStreamsInfo() throws RocksDBException {
        backingStore.putMetadata(streamManagers);
    }

    /**
     * Create a SummaryStore that stores data and indexes in files/directories that
     * start with filePrefix. To store everything in-memory use a null filePrefix
     */
    public SummaryStore(String filePrefix, long cacheSizePerStream) throws RocksDBException {
        this.backingStore = filePrefix != null ?
                new RocksDBBackingStore(filePrefix + ".backingStore", cacheSizePerStream) :
                new MainMemoryBackingStore();
        executorService = Executors.newCachedThreadPool();
        Object uncast = backingStore.getMetadata();
        if (uncast != null) {
            streamManagers = (ConcurrentHashMap<Long, StreamManager>) uncast;
            for (StreamManager si: streamManagers.values()) {
                si.populateTransientFields(backingStore, executorService);
            }
        } else {
            streamManagers = new ConcurrentHashMap<>();
        }
    }

    public SummaryStore(String filePrefix) throws RocksDBException {
        this(filePrefix, 0);
    }

    public SummaryStore() throws RocksDBException {
        this(null);
    }

    public void registerStream(final long streamID, WindowingMechanism windowingMechanism, WindowOperator... operators) throws StreamException, RocksDBException {
        synchronized (streamManagers) {
            if (streamManagers.containsKey(streamID)) {
                 throw new StreamException("attempting to register streamID " + streamID + " multiple times");
            } else {
                streamManagers.put(streamID, new StreamManager(backingStore, executorService, streamID, windowingMechanism, operators));
            }
        }
    }

    private StreamManager getStreamManager(long streamID) throws StreamException {
        StreamManager streamManager = streamManagers.get(streamID);
        if (streamManager == null) {
            throw new StreamException("invalid streamID " + streamID);
        }
        return streamManager;
    }

    public int getOpSequenceForStream(long streamID, OpType opType) throws StreamException{
        return getStreamManager(streamID).getSequenceForOp(opType);
    }

    public Object query(long streamID, long t0, long t1, int aggregateNum, Object... queryParams) throws StreamException, QueryException, RocksDBException {
        if (t0 < 0 || t0 > t1) {
            throw new QueryException("[" + t0 + ", " + t1 + "] is not a valid time interval");
        }
        StreamManager streamManager = getStreamManager(streamID);
        streamManager.lock.readLock().lock();
        try {
            return streamManager.query(aggregateNum, t0, t1, queryParams);
        } finally {
            streamManager.lock.readLock().unlock();
        }
    }

    /** client specified time stamp; client needs to ensure no out-of-order */
    public void append(long streamID, long ts, Object... value) throws StreamException, RocksDBException {
        StreamManager streamManager = getStreamManager(streamID);


        // FIXME: hack for testing; if client does not specify ts, server assigns
        if(ts==-1) {
            ts = Math.abs(System.currentTimeMillis());
        }

        streamManager.lock.writeLock().lock();
        try {
            logger.debug("Appending new value: <ts: " + ts + ", val: " + value + ">");
            streamManager.append(ts, value);
        } finally {
            streamManager.lock.writeLock().unlock();
        }
    }


    /** system assigned time stamp
     *  right now unused since SummaryNode assign ts to messages which don't have a ts from SummaryClients
     */
    /*
    public void append_noTime(long streamID, Object... value) throws StreamException, RocksDBException {
        long ts = System.currentTimeMillis();
        StreamManager streamManager = getStreamManager(streamID);
        streamManager.lock.writeLock().lock();
        try {
            logger.debug("Appending new value: <ts: " + ts + ", val: " + value + ">");
            streamManager.append(ts, value);
        } finally {
            streamManager.lock.writeLock().unlock();
        }
    }
    */

    /**
     * Initiate a landmark window with specified start timestamp. timestamp must be strictly larger than last appended
     * value.
     *
     * Has no effect is there already is an active landmark window.
     */
    public void startLandmark(long streamID, long timestamp) throws StreamException, LandmarkException, RocksDBException {
        StreamManager streamManager = getStreamManager(streamID);
        streamManager.lock.writeLock().lock();
        try {
            streamManager.startLandmark(timestamp);
        } finally {
            streamManager.lock.writeLock().unlock();
        }
    }

    /**
     * Seal the active landmark window, throwing an exception if there isn't one. timestamp must not precede last
     * appended value.
     */
    public void endLandmark(long streamID, long timestamp) throws StreamException, LandmarkException, RocksDBException {
        StreamManager streamManager = getStreamManager(streamID);
        streamManager.lock.writeLock().lock();
        try {
            streamManager.endLandmark(timestamp);
        } finally {
            streamManager.lock.writeLock().unlock();
        }
    }

    public void printWindowState(long streamID, boolean printPerWindowState) throws StreamException, RocksDBException {
        StreamManager streamManager = getStreamManager(streamID);
        System.out.println("Stream " + streamID + " with " + streamManager.stats.getNumValues() + " elements in " +
                streamManager.summaryWindowIndex.size() + " summary windows");
        if (printPerWindowState) {
            for (long swid : streamManager.summaryWindowIndex.values()) {
                System.out.println("\t" + backingStore.getSummaryWindow(streamManager, swid));
            }
            for (long lwid : streamManager.landmarkWindowIndex.values()) {
                System.out.println("\t" + backingStore.getLandmarkWindow(streamManager, lwid));
            }
        }
    }

    public void printWindowState(long streamID) throws StreamException, RocksDBException {
        printWindowState(streamID, false);
    }

    public void warmupCache() throws RocksDBException {
        backingStore.warmupCache(streamManagers);
    }

    public void flush(long streamID) throws RocksDBException, StreamException {
        StreamManager streamManager = getStreamManager(streamID);

        streamManager.lock.writeLock().lock();
        try {
            streamManager.windowingMechanism.flush(streamManager);
        } finally {
            streamManager.lock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws RocksDBException {
        synchronized (streamManagers) {
            // wait for all in-process writes and reads to finish, and seal read index
            for (StreamManager streamManager: streamManagers.values()) {
                streamManager.lock.writeLock().lock();
            }
            // At this point all operations on existing streams will be blocked. New stream
            // creates are already blocked because we're synchronizing on streamManagers
            for (StreamManager streamManager: streamManagers.values()) {
                streamManager.windowingMechanism.close(streamManager);
                backingStore.flushCache(streamManager);
            }
            persistStreamsInfo();
            backingStore.close();
        }
    }

    /**
     * Get number of summary windows in specified stream. Use a null streamID to get total count over all streams
     */
    public long getNumSummaryWindows(Long streamID) {
        try {
            Collection<StreamManager> managers = streamID != null
                    ? Collections.singletonList(getStreamManager(streamID))
                    : streamManagers.values();
            long ret = 0;
            for (StreamManager sm: managers) {
                sm.lock.readLock().lock();
                try {
                    ret += (long) sm.summaryWindowIndex.size();
                } finally {
                    sm.lock.readLock().unlock();
                }
            }
            //TODO: landmark windows
            return ret;
        } catch (StreamException e) {
            e.printStackTrace();
            return -1;
        }
    }

    public StreamStatistics getStreamStatistics(long streamID) throws StreamException {
        StreamManager streamManager;
        synchronized (streamManagers) {
            streamManager = streamManagers.get(streamID);
            if (streamManager == null) {
                throw new StreamException("attempting to get age of unknown stream " + streamID);
            }
        }
        streamManager.lock.readLock().lock();
        try {
            return new StreamStatistics(streamManager.stats);
        } finally {
            streamManager.lock.readLock().unlock();
        }
    }

    public static void main(String[] args) {
        SummaryStore store = null;
        try {
            String storeLoc = "/tmp/tdstore";
            Runtime.getRuntime().exec(new String[]{"sh", "-c", "rm -rf " + storeLoc + "*"}).waitFor();
            store = new SummaryStore(storeLoc);
            //store = new SummaryStore(null);
            long streamID = 0;
            if (!store.streamManagers.containsKey(streamID)) {
                Windowing windowing
                        = new GenericWindowing(new ExponentialWindowLengths(2));
                        //= new RationalPowerWindowing(1, 1);
                store.registerStream(streamID, new CountBasedWBMH(windowing, 33),
                        new SimpleCountOperator(),
                        new CMSOperator(5, 100, 0),
                        new MaxOperator());
                for (long i = 0; i < 1022; ++i) {
                    if (i == 491) {
                        store.startLandmark(streamID, i);
                    }
                    store.append(streamID, i, i % 10, 1000L);
                    if (i == 500) {
                        store.endLandmark(streamID, i);
                    }
                    store.printWindowState(streamID);
                }
                store.flush(streamID);
                store.printWindowState(streamID, true);
            } else {
                store.printWindowState(streamID);
            }
            long t0 = 1, t1 = 511;
            System.out.println("[" + t0 + ", " + t1 + "] count = " + store.query(streamID, t0, t1, 0, 0.95));
            System.out.println("[" + t0 + ", " + t1 + "] freq(8) = " + store.query(streamID, t0, t1, 1, 8L));
            System.out.println("[" + t0 + ", " + t1 + "] max = " + store.query(streamID, t0, t1, 2));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (store != null) {
                try {
                    store.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
