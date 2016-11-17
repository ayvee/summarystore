package com.samsung.sra.DataStore;

import com.samsung.sra.DataStore.Aggregates.CMSOperator;
import com.samsung.sra.DataStore.Aggregates.SimpleCountOperator;
import org.rocksdb.RocksDBException;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Time-decayed aggregate storage
 */
public class SummaryStore implements DataStore {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SummaryStore.class);

    private final BucketStore bucketStore;

    private final ExecutorService executorService;

    // TODO: make a ConcurrentHashMap
    private final HashMap<Long, StreamManager> streamManagers;

    private void persistStreamsInfo() throws RocksDBException {
        bucketStore.putMetadata(streamManagers);
    }

    /**
     * Create a SummaryStore that stores data and indexes in files/directories that
     * start with filePrefix. To store everything in-memory use a null filePrefix
     */
    public SummaryStore(String filePrefix, long cacheSizePerStream) throws RocksDBException {
        this.bucketStore = filePrefix != null ?
                new RocksDBBucketStore(filePrefix + ".bucketStore", cacheSizePerStream) :
                new MainMemoryBucketStore();
        executorService = Executors.newCachedThreadPool();
        Object uncast = bucketStore.getMetadata();
        if (uncast != null) {
            streamManagers = (HashMap<Long, StreamManager>) uncast;
            for (StreamManager si: streamManagers.values()) {
                si.populateTransientFields(bucketStore, executorService);
            }
        } else {
            streamManagers = new HashMap<>();
        }
    }

    public SummaryStore(String filePrefix) throws RocksDBException {
        this(filePrefix, 0);
    }

    public SummaryStore() throws RocksDBException {
        this(null);
    }

    public void registerStream(final long streamID, Object... params) throws StreamException, RocksDBException {
        assert params != null && params.length > 0;
        WindowingMechanism windowingMechanism = (WindowingMechanism) params[0];
        WindowOperator[] operators = new WindowOperator[params.length - 1];
        for (int i = 1; i < params.length; ++i) {
            operators[i-1] = (WindowOperator) params[i];
        }

        synchronized (streamManagers) {
            if (streamManagers.containsKey(streamID)) {
                 throw new StreamException("attempting to register streamID " + streamID + " multiple times");
            } else {
                streamManagers.put(streamID, new StreamManager(bucketStore, executorService, streamID, windowingMechanism, operators));
            }
        }
    }

    @Override
    public Object query(long streamID, long t0, long t1, int aggregateNum, Object... queryParams) throws StreamException, QueryException, RocksDBException {
        if (t0 < 0 || t0 > t1) {
            throw new QueryException("[" + t0 + ", " + t1 + "] is not a valid time interval");
        }
        final StreamManager streamManager;
        synchronized (streamManagers) {
            if (!streamManagers.containsKey(streamID)) {
                throw new StreamException("attempting to read from unregistered stream " + streamID);
            } else {
                streamManager = streamManagers.get(streamID);
            }
        }
        streamManager.lock.readLock().lock();
        try {
            return streamManager.query(aggregateNum, t0, t1, queryParams);
        } finally {
            streamManager.lock.readLock().unlock();
        }
    }

    public void append(long streamID, long ts, Object... value) throws StreamException, RocksDBException {
        final StreamManager streamManager;
        synchronized (streamManagers) {
            if (!streamManagers.containsKey(streamID)) {
                throw new StreamException("attempting to append to unregistered stream " + streamID);
            } else {
                streamManager = streamManagers.get(streamID);
            }
        }

        streamManager.lock.writeLock().lock();
        try {
            //logger.debug("Appending new value: <ts: " + ts + ", val: " + value + ">");
            streamManager.append(ts, value);
        } finally {
            streamManager.lock.writeLock().unlock();
        }
    }

    public void printBucketState(long streamID, boolean printPerBucketState) throws RocksDBException {
        StreamManager streamManager = streamManagers.get(streamID);
        System.out.println("Stream " + streamID + " with " + streamManager.stats.getNumValues() + " elements in " +
                streamManager.temporalIndex.size() + " windows");
        if (printPerBucketState) {
            for (Object bucketID : streamManager.temporalIndex.values()) {
                System.out.println("\t" + bucketStore.getBucket(streamManager, (long) bucketID));
            }
        }
    }

    public void printBucketState(long streamID) throws RocksDBException {
        printBucketState(streamID, false);
    }

    public void warmupCache() throws RocksDBException {
        bucketStore.warmupCache(streamManagers);
    }

    @Override
    public void flush(long streamID) throws RocksDBException, StreamException {
        final StreamManager streamManager;
        synchronized (streamManagers) {
            if (!streamManagers.containsKey(streamID)) {
                throw new StreamException("attempting to flush unregistered stream " + streamID);
            } else {
                streamManager = streamManagers.get(streamID);
            }
        }

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
                bucketStore.flushCache(streamManager);
            }
            persistStreamsInfo();
            bucketStore.close();
        }
    }

    /**
     * Get number of windows (buckets) in specified stream. Use a null streamID to get total count over all streams
     */
    public long getNumWindows(Long streamID) {
        // FIXME: does not sanity-check streamID argument, unlike every other SummaryStore function
        Collection<StreamManager> managers = streamID != null
                ? Collections.singletonList(streamManagers.get(streamID))
                : streamManagers.values();
        long ret = 0;
        for (StreamManager sm: managers) {
            sm.lock.readLock().lock();
            try {
                ret += (long) sm.temporalIndex.size();
            } finally {
                sm.lock.readLock().unlock();
            }
        }
        return ret;
    }

    @Override
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
                //store.registerStream(streamID, new CountBasedWBMH(streamID, new GenericWindowing(new ExponentialWindowLengths(2))));
                Windowing windowing
                        = new GenericWindowing(new ExponentialWindowLengths(2));
                        //= new RationalPowerWindowing(1, 1);
                store.registerStream(streamID, new CountBasedWBMH(windowing, 33),
                        new SimpleCountOperator(),
                        new CMSOperator(5, 100, 0));
                for (long i = 0; i < 1022; ++i) {
                    store.append(streamID, i, i % 10, 1000L);
                    store.printBucketState(streamID);
                }
                store.flush(streamID);
                store.printBucketState(streamID, true);
            } else {
                store.printBucketState(streamID);
            }
            long t0 = 1, t1 = 511;
            System.out.println(
                    "[" + t0 + ", " + t1 + "] count = " + store.query(streamID, t0, t1, 0, 0.95));
            System.out.println(
                    "[" + t0 + ", " + t1 + "] freq(8) = " + store.query(streamID, t0, t1, 1, 8L));
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
