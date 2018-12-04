/*
* Copyright 2016 Samsung Research America. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.samsung.sra.datastore;

import com.samsung.sra.datastore.ingest.CountBasedWBMH;
import com.samsung.sra.datastore.storage.BackingStore;
import com.samsung.sra.datastore.storage.BackingStoreException;
import com.samsung.sra.datastore.storage.MainMemoryBackingStore;
import com.samsung.sra.datastore.storage.RocksDBBackingStore;
import com.samsung.sra.protocol.OpTypeOuterClass;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
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
    private static final String EXTERNAL_AUX_PREFIX = "EXTERNAL_";

    private final BackingStore backingStore;
    private final String directory;
    private final Options options;

    public static class Options {
        private boolean keepReadIndexes = true;
        private boolean readonly = false;
        private boolean lazyload = false;
        private long cacheSizePerStream = 0;

        /**
         * Maintain an in-memory index over window IDs for each stream. Needed to use caching when using RocksDB backing
         * store
         */
        public Options setKeepReadIndexes(boolean keepReadIndexes) {
            this.keepReadIndexes = keepReadIndexes;
            return this;
        }

        /** Open in read-only mode */
        public Options setReadOnly(boolean readonly) {
            this.readonly = readonly;
            return this;
        }

        /**
         * Number of summary windows to cache in memory for each stream. Set to 0 to disable caching. With RocksDB
         * backing store, only allowed if (keepReadIndexes && readonly)
         */
        public Options setCacheSizePerStream(long cacheSizePerStream) {
            this.cacheSizePerStream = cacheSizePerStream >= 0 ? cacheSizePerStream : 0;
            return this;
        }

        public Options setLazyLoad(boolean lazyload) {
            this.lazyload = lazyload;
            return this;
        }
    }

    ConcurrentHashMap<Long, Stream> streams; // package-local rather than private to allow access from SummaryStoreTest

    /**
     * @param directory  Directory to store all summary store data/indexes in. Set to null to use in-memory store
     * @param options  Store options
     */
    public SummaryStore(String directory, Options options) throws BackingStoreException, IOException, ClassNotFoundException {
        this.options = options;
        if (options.cacheSizePerStream > 0 && !(options.keepReadIndexes && options.readonly)) {
            throw new IllegalArgumentException("Backing store cache not allowed in read/write mode (use the memory for" +
                    " ingest buffer instead)");
        }
        if (directory != null) {
            File dir = new File(directory);
            if (!dir.exists()) {
                boolean created = dir.mkdirs();
                assert created;
            }
            this.backingStore = new RocksDBBackingStore(directory + "/rocksdb", options.cacheSizePerStream, options.readonly);
            this.directory = directory;
        } else {
            this.backingStore = new MainMemoryBackingStore();
            this.directory = null;
        }
        deserializeMetadata();
    }

    /**
     * @param directory  Directory to store all summary store data/indexes in. Set to null to use in-memory store
     */
    public SummaryStore(String directory) throws BackingStoreException, IOException, ClassNotFoundException {
        this(directory, new Options());
    }

    /**
     * getAux and putAux are for retrieving and persistently storing auxiliary info, such as stream statistics, stream
     * metadata, Snode info etc. Currently not very optimized, not advisable to store large or frequently updated
     * objects here.
     */
    public byte[] getAux(String key) throws BackingStoreException {
        return key == null ? null : getAuxInternal(EXTERNAL_AUX_PREFIX + key);
    }

    /**
     * getAux and putAux are for retrieving and persistently storing auxiliary info, such as stream statistics, stream
     * metadata, Snode info etc. Currently not very optimized, not advisable to store large or frequently updated
     * objects here.
     */
    public void putAux(String key, byte[] value) throws BackingStoreException {
        if (key != null) {
            putAuxInternal(EXTERNAL_AUX_PREFIX + key, value);
        }
    }

    private byte[] getAuxInternal(String key) throws BackingStoreException {
        return backingStore.getAux(key);
    }

    private void putAuxInternal(String key, byte[] value) throws BackingStoreException {
        backingStore.putAux(key, value);
    }

    /** Unload stream indexes etc to disk */
    public void unloadStream(long streamID) throws StreamException, IOException, BackingStoreException {
        getStream(streamID).unload(directory);
    }

    /** Load stream into main memory. FIXME: only loads into readonly mode right now (read/write reload is buggy) */
    public void loadStream(long streamID) throws IOException, ClassNotFoundException, StreamException {
        Stream stream = streams.get(streamID);
        if (stream == null) {
            throw new StreamException("attempting to load unknown stream " + streamID);
        }
        stream.load(directory, true, backingStore);
    }

    private void serializeMetadata() throws IOException, BackingStoreException {
        putAuxInternal("metadata", Utilities.serialize(streams));
        //Utilities.serializeObject(directory + "/metadata", streams);
        for (Stream stream : streams.values()) {
            stream.unload(directory);
        }
    }

    private void deserializeMetadata() throws IOException, ClassNotFoundException {
        try {
            streams = Utilities.deserialize(getAuxInternal("metadata"));
        } catch (Exception e) {
            logger.debug("Could not read metadata, initializing assuming empty store");
            streams = new ConcurrentHashMap<>();
            return;
        }
        /*if (directory == null || !(new File(directory + "/metadata").exists())) {
            streams =  new ConcurrentHashMap<>();
            return;
        }
        streams = Utilities.deserializeObject(directory + "/metadata");*/
        if (!options.lazyload) {
            for (Stream stream : streams.values()) {
                stream.load(directory, options.readonly, backingStore);
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
                logger.warn("Attempting to register stream {} multiple times, ignoring", streamID);
                // can happen during distributed bootup; warn instead of throwing exception
                //throw new StreamException("attempting to register streamID " + streamID + " multiple times");
            } else {
                Stream sm = new Stream(streamID, synchronizeWrites, wbmh, operators, options.keepReadIndexes);
                sm.populateTransientFields(backingStore);
                streams.put(streamID, sm);
            }
        }
    }

    private Stream getStream(long streamID) throws StreamException {
        Stream stream = streams.get(streamID);
        if (stream == null) {
            throw new StreamException("invalid streamID " + streamID);
        } else if (!stream.isLoaded()) {
            throw new StreamException("attempting to access unloaded stream " + streamID + " (call store.load(streamID))");
        }
        return stream;
    }

    /**
     * Get the operator index of the specified operator. Throws StreamException if the stream does not exist or does not
     * have an operator of type opType.
     */
    public int getOperatorIndex(long streamID, OpTypeOuterClass.OpType opType) throws StreamException {
        return getStream(streamID).getOperatorIndex(opType);
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

    public void appendAutoTimestamped(long streamID, Object value) throws StreamException, BackingStoreException {
        append(streamID, System.currentTimeMillis(), value);
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

    public void printStoreState() {
        logger.info(streams.size() + " streams");
        for (Stream s : streams.values()) {
            logger.info("Stream {}: {} values, time range [{}:{}]", s.streamID, s.stats.getNumValues(),
                    s.stats.getTimeRangeStart(), s.stats.getTimeRangeEnd());
        }
    }

    public void flush(long streamID) throws BackingStoreException, StreamException {
        getStream(streamID).flush();
    }

    @Override
    public void close() throws BackingStoreException, IOException {
        synchronized (streams) { // this blocks creating new streams
            if (!options.readonly) {
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
