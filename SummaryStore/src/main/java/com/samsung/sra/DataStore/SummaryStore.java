package com.samsung.sra.DataStore;

import com.samsung.sra.DataStore.Ingest.CountBasedWBMH;
import com.samsung.sra.DataStore.Storage.BackingStore;
import com.samsung.sra.DataStore.Storage.BackingStoreException;
import com.samsung.sra.DataStore.Storage.MainMemoryBackingStore;
import com.samsung.sra.DataStore.Storage.RocksDBBackingStore;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Start here. Most external code will only construct and interact with an instance of this class.
 *
 * All calls that modify a stream (append, landmark, flush, close) must be serialized. If calling code cannot do it on
 * its own it must set a flag in registerStream to have us use a lock.
 *
 * This class forwards all API calls to Stream.
 */
public class SummaryStore implements AutoCloseable {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SummaryStore.class);

    private final BackingStore backingStore;
    private final String indexesFile;

    final ConcurrentHashMap<Long, Stream> streams; // package-local rather than private to allow access from SummaryStoreTest
    private final boolean readonly;

    /**
     * Create a SummaryStore that stores data and indexes in files/directories that
     * start with filePrefix. To store everything in-memory use a null filePrefix
     */
    public SummaryStore(String filePrefix, long cacheSizePerStream, boolean readonly)
            throws BackingStoreException, IOException, ClassNotFoundException {
        if (cacheSizePerStream > 0 && !readonly) {
            throw new IllegalArgumentException("Backing store cache not allowed in read/write mode (use the memory for" +
                    " ingest buffer instead)");
        }
        if (filePrefix != null) {
            this.backingStore = new RocksDBBackingStore(filePrefix + ".backingStore", cacheSizePerStream);
            this.indexesFile = filePrefix + ".indexes";
        } else {
            this.backingStore = new MainMemoryBackingStore();
            this.indexesFile = null;
        }
        this.readonly = readonly;
        streams = deserializeIndexes();
    }

    public SummaryStore(String filePrefix, long cacheSizePerStream)
            throws BackingStoreException, IOException, ClassNotFoundException {
        this(filePrefix, cacheSizePerStream, false);
    }

    public SummaryStore(String filePrefix) throws BackingStoreException, IOException, ClassNotFoundException {
        this(filePrefix, 0);
    }

    public SummaryStore() throws BackingStoreException, IOException, ClassNotFoundException {
        this(null);
    }

    private void serializeIndexes() throws IOException {
        if (indexesFile == null) return;
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(indexesFile))) {
            oos.writeObject(streams);
        }
    }

    private ConcurrentHashMap<Long, Stream> deserializeIndexes() throws IOException, ClassNotFoundException {
        File file;
        if (indexesFile == null || !(file = new File(indexesFile)).exists()) {
            return new ConcurrentHashMap<>();
        } else {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
                ConcurrentHashMap<Long, Stream> ret = (ConcurrentHashMap<Long, Stream>) ois.readObject();
                for (Stream si: ret.values()) {
                    si.populateTransientFields(backingStore);
                }
                return ret;
            }
        }
    }

    public void registerStream(final long streamID, CountBasedWBMH wbmh, WindowOperator... operators)
            throws BackingStoreException, StreamException {
        registerStream(streamID, true, wbmh, operators);
    }

    /**
     * Register a stream with specified windowing and operators. Set the optional synchronizeWrites flag to false to
     * disable the internal lock we otherwise use to serialize all append/landmark/flush/close calls
     */
    public void registerStream(final long streamID, boolean synchronizeWrites,
                               CountBasedWBMH wbmh, WindowOperator... operators)
            throws StreamException, BackingStoreException {
        synchronized (streams) {
            if (streams.containsKey(streamID)) {
                 throw new StreamException("attempting to register streamID " + streamID + " multiple times");
            } else {
                Stream sm = new Stream(streamID, synchronizeWrites, wbmh, operators);
                sm.populateTransientFields(backingStore);
                streams.put(streamID, sm);
            }
        }
    }

    private Stream getStream(long streamID) throws StreamException {
        Stream stream = streams.get(streamID);
        if (stream == null) {
            throw new StreamException("invalid streamID " + streamID);
        }
        return stream;
    }

    public Object query(long streamID, long t0, long t1, int aggregateNum, Object... queryParams)
            throws StreamException, BackingStoreException {
        if (t0 < 0 || t0 > t1) {
            throw new IllegalArgumentException("[" + t0 + ", " + t1 + "] is not a valid time interval");
        }
        return getStream(streamID).query(aggregateNum, t0, t1, queryParams);
    }

    public void append(long streamID, long ts, Object value) throws StreamException, BackingStoreException {
        getStream(streamID).append(ts, value);
    }

    /**
     * Initiate a landmark window with specified start timestamp. timestamp must be strictly larger than last appended
     * value.
     *
     * Has no effect is there already is an active landmark window.
     */
    public void startLandmark(long streamID, long timestamp) throws StreamException, BackingStoreException {
        getStream(streamID).startLandmark(timestamp);
    }

    /**
     * Seal the active landmark window, throwing an exception if there isn't one. timestamp must not precede last
     * appended value.
     */
    public void endLandmark(long streamID, long timestamp) throws StreamException, BackingStoreException {
        getStream(streamID).endLandmark(timestamp);
    }

    public void printWindowState(long streamID) throws StreamException, BackingStoreException {
        getStream(streamID).printWindows();
    }

    public void flush(long streamID) throws BackingStoreException, StreamException {
        getStream(streamID).flush();
    }

    @Override
    public void close() throws BackingStoreException, IOException {
        synchronized (streams) { // this blocks creating new streams
            if (!readonly) {
                for (Stream stream : streams.values()) {
                    stream.close();
                }
                serializeIndexes();
            }
            backingStore.close();
        }
    }

    /**
     * Get number of summary windows in specified stream. Use a null streamID to get total count over all streams
     */
    public long getNumSummaryWindows(Long streamID) throws StreamException, BackingStoreException {
        Collection<Stream> streams = streamID != null
                ? Collections.singletonList(getStream(streamID))
                : this.streams.values();
        long ret = 0;
        for (Stream sm : streams) {
            ret += sm.getNumSummaryWindows();
        }
        return ret;
    }

    /**
     * Get number of landmark windows in specified stream. Use a null streamID to get total count over all streams
     */
    public long getNumLandmarkWindows(Long streamID) throws StreamException {
        Collection<Stream> streams = streamID != null
                ? Collections.singletonList(getStream(streamID))
                : this.streams.values();
        long ret = 0;
        for (Stream sm : streams) {
            ret += sm.getNumLandmarkWindows();
        }
        return ret;
    }

    public StreamStatistics getStreamStatistics(long streamID) throws StreamException {
        return new StreamStatistics(getStream(streamID).stats);
    }
}
