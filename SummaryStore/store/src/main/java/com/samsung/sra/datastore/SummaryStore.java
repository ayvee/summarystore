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
import java.io.Serializable;
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
    private final StoreOptions storeOptions;

    public static class StoreOptions implements Serializable {
        private boolean keepReadIndexes = true;
        private boolean readonly = false;
        private boolean lazyload = false;
        private long cacheSizePerStream = 0;
        private int commonMergePoolSize = 0;

        /**
         * <p>Maintain an in-memory index to speed up reads. Default true. Disabling this saves index-maintenance overhead
         * and reduces memory footprint.</p>
         *
         * <p>Must be enabled from the beginning if we ever expect to use a read-cache (now or in the future). It is
         * currently not possible to build a read index post-facto, on demand.</p>
         */
        public StoreOptions setKeepReadIndexes(boolean keepReadIndexes) {
            this.keepReadIndexes = keepReadIndexes;
            return this;
        }

        /** Open store in read-only mode. Default false. */
        public StoreOptions setReadOnly(boolean readonly) {
            if (!readonly && cacheSizePerStream > 0) {
                throw new IllegalArgumentException("Read cache is only allowed in read-only mode");
            }
            this.readonly = readonly;
            return this;
        }

        /**
         * Size (# windows) of in-memory cache used when serving read queries. Implies and only allowed in read-only
         * mode. Default 0 (caching disabled).
         */
        public StoreOptions setReadCacheSizePerStream(long cacheSizePerStream) {
            setReadOnly(true);
            this.cacheSizePerStream = cacheSizePerStream >= 0 ? cacheSizePerStream : 0;
            return this;
        }

        /**
         * Only load a particular stream's metadata and indexes from disk when they are referenced. Efficient if we
         * expect to have very large numbers of streams (e.g. millions). Default false, meaning all streams' info will
         * always be open in memory.
         */
        public StoreOptions setLazyLoad(boolean lazyload) {
            this.lazyload = lazyload;
            return this;
        }

        /**
         * <p>Use a thread pool with {@code nThreads} threads to handle window merges. The thread pool will be
         * shared by all streams; this is a global option. Default 0, meaning one separate merge thread per stream.</p>
         *
         * <p>FIXME: currently uses the global Java parallel stream ForkJoinPool for merges; this option sets that
         * pool's parallelism level. Should really use a separate ExecutorService.</p>
         */
        public StoreOptions setSharedWindowMergePool(int nThreads) {
            commonMergePoolSize = nThreads;
            if (commonMergePoolSize > 1) {
                System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", Integer.toString(nThreads));
            }
            return this;
        }
    }

    public static class StreamOptions implements Serializable {
        private int ingestBufferSize = 0;
        private boolean valuesAreLongs = false;
        private long windowMergeFrequency = 1;
        private boolean useWriteLock = true;

        /**
         * <p>For efficiency, buffer up to {@code ingestBufferSize} (time, value) pairs in memory before processing
         * them into the store as a batch. Default 0. For high-velocity ingest, make this as large as memory allows.</p>
         *
         * <p>Note that if this is > 0, queries will not read any newly inserted values still in the buffer. Call
         * {@link SummaryStore#flush(long)} to flush values from the ingest buffer into the store.</p>
         */
        public StreamOptions setIngestBufferSize(int ingestBufferSize) {
            this.ingestBufferSize = ingestBufferSize;
            return this;
        }
        /**
         * Only merge windows once every W full window appends. Default 1. Higher values mean less frequent
         * merge compaction, and thus less I/O pressure due to merge, at the cost of higher transient memory/disk
         * footprint.
         */
        public StreamOptions setWindowMergeFrequency(long W) {
            if (W < 1) {
                throw new IllegalArgumentException("window merge frequency must be >= 1");
            }
            this.windowMergeFrequency = W;
            return this;
        }

        /**
         * If false, disable the internal lock we normally use to serialize mutating operations; the calling client will
         * instead have to manually ensure that all append/startLandmark/endLandmark/flush calls to the stream are
         * serialized. Default true, meaning use an internal lock.
         */
        public StreamOptions setUseWriteLock(boolean useWriteLock) {
            this.useWriteLock = useWriteLock;
            return this;
        }

        /**
         * Niche option. If the client is only ever going to insert 64-bit long values for this stream, enable to turn
         * on a fast code path that uses off-heap buffers to increase speed and reduce GC pressure. WARNING: will lead
         * to an exception if a non-long value is ever inserted.  Default false.
         */
        public StreamOptions setValuesAreLongs(boolean valuesAreLongs) {
            this.valuesAreLongs = valuesAreLongs;
            return this;
        }

        public int getIngestBufferSize() {
            return ingestBufferSize;
        }

        public long getWindowMergeFrequency() {
            return windowMergeFrequency;
        }

        public boolean getUseWriteLock() {
            return useWriteLock;
        }

        public boolean getValuesAreLongs() {
            return valuesAreLongs;
        }
    }

    ConcurrentHashMap<Long, Stream> streams; // package-local rather than private to allow access from SummaryStoreTest

    /**
     * @param directory  Directory to store all summary store data/indexes in. Set to null to use transient in-memory store
     */
    public SummaryStore(String directory, StoreOptions storeOptions) throws BackingStoreException, IOException, ClassNotFoundException {
        this.storeOptions = storeOptions;
        if (storeOptions.cacheSizePerStream > 0 && !(storeOptions.keepReadIndexes && storeOptions.readonly)) {
            throw new IllegalArgumentException("Read cache not allowed in read/write mode (use the memory for" +
                    " ingest buffer instead)");
        }
        if (directory != null) {
            File dir = new File(directory);
            if (!dir.exists()) {
                boolean created = dir.mkdirs();
                assert created;
            }
            this.backingStore = new RocksDBBackingStore(directory + "/rocksdb", storeOptions.cacheSizePerStream, storeOptions.readonly);
            this.directory = directory;
        } else {
            this.backingStore = new MainMemoryBackingStore();
            this.directory = null;
        }
        deserializeMetadata();
    }

    /**
     * Open a SummaryStore instance with default options.
     *
     * @param directory  Directory to store all summary store data/indexes in. Set to null to use transient in-memory store
     */
    public SummaryStore(String directory) throws BackingStoreException, IOException, ClassNotFoundException {
        this(directory, new StoreOptions());
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

    /**
     * Unload all info for a stream (metadata, indexes etc) to disk. No writes/reads to the stream are possible until
     * the stream is loaded again. */
    public void unloadStream(long streamID) throws StreamException, IOException, BackingStoreException {
        getStream(streamID).unload(directory);
    }

    /**
     * Load stream info, rendering it available for read/write.
     * FIXME: only loads into readonly mode right now (read/write reload is buggy) */
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
        if (!storeOptions.lazyload) {
            for (Stream stream : streams.values()) {
                stream.load(directory, storeOptions.readonly, backingStore);
            }
        }
    }

    /**
     * Register a stream with specified windowing and operators and default stream options.
     * @param streamID  Unique stream ID.
     * @param windowing  Window length sequence (decay function).
     * @param operators  List of summary structures to maintain for the stream.
     */
    public void registerStream(final long streamID, Windowing windowing, WindowOperator... operators) {
        registerStream(streamID, new StreamOptions(), windowing, operators);
    }

    /**
     * Register a stream with specified windowing, operators and stream options.
     * @param streamID  Unique stream ID.
     * @param streamOptions  Stream-specific options.
     * @param windowing  Window length sequence (decay function).
     * @param operators  List of summary structures to maintain for the stream.
     */
    public void registerStream(final long streamID, StreamOptions streamOptions, Windowing windowing,
                               WindowOperator... operators) {
        synchronized (streams) {
            if (streams.containsKey(streamID)) {
                logger.warn("Attempting to register stream {} multiple times, ignoring", streamID);
                // can happen during distributed bootup; warn instead of throwing exception
                //throw new StreamException("attempting to register streamID " + streamID + " multiple times");
            } else {
                CountBasedWBMH wbmh = new CountBasedWBMH(windowing, streamOptions, storeOptions.commonMergePoolSize > 1);
                Stream sm = new Stream(streamID, streamOptions.getUseWriteLock(), wbmh, operators, storeOptions.keepReadIndexes);
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

    public Object query(long streamID, long t0, long t1, int operatorIndex, Object... queryParams)
            throws StreamException, BackingStoreException {
        if (t0 < 0 || t0 > t1) {
            throw new IllegalArgumentException("[" + t0 + ", " + t1 + "] is not a valid time interval");
        }
        return getStream(streamID).query(operatorIndex, t0, t1, queryParams);
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
            if (!storeOptions.readonly) {
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
