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
    private final String directory;

    ConcurrentHashMap<Long, Stream> streams; // package-local rather than private to allow access from SummaryStoreTest
    private final boolean keepReadIndexes;
    private final boolean readonly;

    /**
     * @param directory  Directory to store all summary store data/indexes in. Set to null to use in-memory store
     * @param keepReadIndexes  Maintain an in-memory index over window IDs for each stream. Needed to use caching when
     *                         using RocksDB backing store
     * @param readonly  Open in read-only mode
     * @param cacheSizePerStream  Number of summary windows to cache in memory for each stream. Set to 0 to disable
     *                            caching. With RocksDB backing store, only allowed if (keepReadIndexes && readonly)
     */
    public SummaryStore(String directory, boolean keepReadIndexes, boolean readonly, long cacheSizePerStream)
            throws BackingStoreException, IOException, ClassNotFoundException {
        if (cacheSizePerStream > 0 && !(keepReadIndexes && readonly)) {
            throw new IllegalArgumentException("Backing store cache not allowed in read/write mode (use the memory for" +
                    " ingest buffer instead)");
        }
        if (directory != null) {
            File dir = new File(directory);
            if (!dir.exists()) {
                boolean created = dir.mkdirs();
                assert created;
            }
            this.backingStore = new RocksDBBackingStore(directory + "/rocksdb", cacheSizePerStream);
            this.directory = directory;
        } else {
            this.backingStore = new MainMemoryBackingStore();
            this.directory = null;
        }
        this.keepReadIndexes = keepReadIndexes;
        this.readonly = readonly;
        deserializeMetadata();
    }

    public SummaryStore(String directory) throws BackingStoreException, IOException, ClassNotFoundException {
        this(directory, true, false, 0);
    }

    private void serializeMetadata() throws IOException {
        if (directory == null) return;
        serializeObject(directory + "/metadata", streams);
        for (Stream stream : streams.values()) {
            serializeObject(directory + "/read-index." + stream.streamID, stream.windowManager);
            serializeObject(directory + "/write-index." + stream.streamID, stream.wbmh);
        }
    }

    private void deserializeMetadata() throws IOException, ClassNotFoundException {
        if (directory == null || !(new File(directory + "/metadata").exists())) {
            streams =  new ConcurrentHashMap<>();
            return;
        }
        streams = deserializeObject(directory + "/metadata");
        for (Stream stream : streams.values()) {
            stream.windowManager = deserializeObject(directory + "/read-index." + stream.streamID);
            stream.wbmh = readonly ? null
                    : deserializeObject(directory + "/write-index." + stream.streamID);
            stream.populateTransientFields(backingStore);
        }
    }

    private static <T> void serializeObject(String filename, T obj) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filename))) {
            oos.writeObject(obj);
        }
    }

    private static <T> T deserializeObject(String filename) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream((new FileInputStream(filename)))) {
            return (T) ois.readObject();
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
                Stream sm = new Stream(streamID, synchronizeWrites, wbmh, operators, keepReadIndexes);
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
                serializeMetadata();
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
