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
package com.samsung.sra.datastore.storage;

import com.samsung.sra.datastore.LandmarkWindow;
import com.samsung.sra.datastore.SummaryWindow;
import com.samsung.sra.datastore.Utilities;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class RocksDBBackingStore extends BackingStore {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBBackingStore.class);

    private final RocksDB rocksDB;
    private final Options rocksDBOptions;
    private final WriteOptions rocksDBWriteOptions;
    private final long cacheSizePerStream;
    /** Map streamID -> windowID -> window */
    private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, SummaryWindow>> cache;

    /**
     * @param rocksPath  on-disk path
     * @param cacheSizePerStream  number of elements per stream to cache in main memory. Set to 0 to disable caching.
     *                            Should only be used in readonly mode
     * @throws BackingStoreException  wrapping RocksDBException
     */
    public RocksDBBackingStore(String rocksPath, long cacheSizePerStream, boolean readonly) throws BackingStoreException {
        this.cacheSizePerStream = cacheSizePerStream;
        cache = cacheSizePerStream > 0 ? new ConcurrentHashMap<>() : null;
        // FIXME: take from external conf rather than hard-coding. Current settings are for our default test hardware
        rocksDBOptions = new Options()
                .setCreateIfMissing(true)
                .setDbLogDir("/tmp")
                .createStatistics()
                .setStatsDumpPeriodSec(300)
                .setMaxBackgroundCompactions(10)
                .setMaxBackgroundFlushes(10)
                .setAllowConcurrentMemtableWrite(true)
                .setDbWriteBufferSize(512L * 1024 * 1024)
                .setMaxWriteBufferNumber(16)
                .setMinWriteBufferNumberToMerge(4)
                .setLevel0FileNumCompactionTrigger(4)
                .setLevel0SlowdownWritesTrigger(10_000)
                .setLevel0StopWritesTrigger(10_000)
                .setSoftPendingCompactionBytesLimit(1_000_000_000_000_000L)
                .setHardPendingCompactionBytesLimit(1_000_000_000_000_000L)
                .setMaxBytesForLevelBase(512L * 1024 * 1024 * 4 * 4)
                .setTargetFileSizeBase(512L * 1024 * 1024)
                .setLevelCompactionDynamicLevelBytes(true)
                .setCompactionReadaheadSize(10L * 1024 * 1024)
                .setNewTableReaderForCompactionInputs(true)
                //.setCompactionStyle(CompactionStyle.UNIVERSAL)
                //.setCompressionType(CompressionType.NO_COMPRESSION)
                //.setMemTableConfig(new VectorMemTableConfig())
                .setTableFormatConfig(new BlockBasedTableConfig()
                        .setBlockSize(256L * 1024)
                        .setBlockCacheSize(readonly
                                ? 512 * 1024 * 1024L
                                : 30 * 1024 * 1024 * 1024L)
                        .setCacheIndexAndFilterBlocks(true)
                        //.setFilter(new BloomFilter())
                )
                //.setOptimizeFiltersForHits(true)
                .setMaxOpenFiles(-1);
        rocksDBWriteOptions = new WriteOptions()
                .setDisableWAL(true);
        try {
            rocksDB = readonly
                    ? RocksDB.openReadOnly(rocksDBOptions, rocksPath)
                    : RocksDB.open(rocksDBOptions, rocksPath);
        } catch (RocksDBException e) {
            throw new BackingStoreException(e);
        }
    }

    static {
        RocksDB.loadLibrary();
    }

    private static final int KEY_SIZE = 16;

    /**
     * RocksDB key = <streamID, windowID>. Since we ensure windowIDs are assigned in increasing
     * order, this lays out data in temporal order within streams
     */
    private static byte[] getRocksDBKey(long streamID, long windowID) {
        byte[] keyArray = new byte[KEY_SIZE];
        Utilities.longToByteArray(streamID, keyArray, 0);
        Utilities.longToByteArray(windowID, keyArray, 8);
        return keyArray;
    }

    private static long getStreamIDFromRocksDBKey(byte[] key) {
        return Utilities.byteArrayToLong(key, 0);
    }

    private static long getWindowIDFromRocksDBKey(byte[] key) {
        return Utilities.byteArrayToLong(key, 8);
    }

    private void insertIntoCache(ConcurrentHashMap<Long, SummaryWindow> streamCache, long swid, SummaryWindow window) {
        if (streamCache.size() >= cacheSizePerStream) { // evict random
            Map.Entry<Long, SummaryWindow> evictedEntry = streamCache.entrySet().iterator().next(); // basically a random evict
            streamCache.remove(evictedEntry.getKey());
        }
        streamCache.put(swid, window);
    }

    @Override
    SummaryWindow getSummaryWindow(long streamID, long swid, SerDe serDe) throws BackingStoreException {
        ConcurrentHashMap<Long, SummaryWindow> streamCache;
        if (cache == null) {
            streamCache = null;
        } else {
            streamCache = cache.get(streamID);
            if (streamCache == null) cache.put(streamID, streamCache = new ConcurrentHashMap<>());
        }

        SummaryWindow window = streamCache != null ? streamCache.get(swid) : null;
        if (window != null) { // cache hit
            return window;
        } else { // either no cache or cache miss; read-through from RocksDB
            byte[] rocksKey = getRocksDBKey(streamID, swid);
            try {
                byte[] rocksValue = rocksDB.get(rocksKey);
                window = serDe.deserializeSummaryWindow(rocksValue);
            } catch (RocksDBException e) {
                throw new BackingStoreException(e);
            }
            if (streamCache != null) insertIntoCache(streamCache, swid, window);
            return window;
        }
    }

    @Override
    void deleteSummaryWindow(long streamID, long swid, SerDe serDe) throws BackingStoreException {
        assert cache == null;
        try {
            byte[] key = getRocksDBKey(streamID, swid);
            rocksDB.delete(key);
        } catch (RocksDBException e) {
            throw new BackingStoreException(e);
        }
    }

    @Override
    void putSummaryWindow(long streamID, long swid, SerDe serDe, SummaryWindow window) throws BackingStoreException {
        assert cache == null;
        try {
            byte[] key = getRocksDBKey(streamID, swid);
            byte[] value = serDe.serializeSummaryWindow(window);
            rocksDB.put(rocksDBWriteOptions, key, value);
        } catch (RocksDBException e) {
            throw new BackingStoreException(e);
        }
    }

    /** Iterate over and return all summary windows in RocksDB overlapping the time-range given in the constructor */
    private class OverlappingRocksIterator implements Iterator<SummaryWindow> {
        private final RocksIterator rocksIterator;
        private SummaryWindow nextWindow;

        private final long streamID;
        private final SerDe serde;
        private final long t0, t1;

        private OverlappingRocksIterator(long streamID, long t0, long t1, SerDe serde) throws RocksDBException {
            this.streamID = streamID;
            this.serde = serde;
            this.t0 = t0;
            this.t1 = t1;

            // Note that Stream.query() ensures stream (1) is non-empty, (2) time interval [T0, T1] fully covers [t0, t1]
            rocksIterator = rocksDB.newIterator();
            rocksIterator.seek(getRocksDBKey(streamID, t0));
            assert rocksIterator.isValid();
            nextWindow = readFromRocksIterator();
            if (nextWindow == null) { // only one window in stream and t0 > T0
                rocksIterator.seek(getRocksDBKey(streamID, 0));
                nextWindow = readFromRocksIterator();
                assert nextWindow != null;
            }
            assert nextWindow.ts >= t0;
            /* rocksIterator now points to the first window with start timestamp >= t0. If timestamp == t0, we only
             * need to return this window and its successors. If timestamp > t0, we also need to return the window just
             * before this one (which is the last window with start timestamp < t0), so we call iterator.prev() */
            if (nextWindow.ts > t0) {
                rocksIterator.prev();
                nextWindow = readFromRocksIterator();
                assert nextWindow != null;
            }
        }

        private SummaryWindow readFromRocksIterator() {
            if (rocksIterator.isValid()) {
                byte[] key = rocksIterator.key();
                if (key.length == KEY_SIZE) {
                    long streamID = getStreamIDFromRocksDBKey(key), ts = getWindowIDFromRocksDBKey(key);
                    if (streamID == this.streamID && ts <= t1) {
                        return serde.deserializeSummaryWindow(rocksIterator.value());
                    }
                }
            }
            // at least one of the "iterator is valid" conditions must have failed
            rocksIterator.close();
            return null;
        }

        @Override
        public boolean hasNext() {
            return nextWindow != null;
        }

        @Override
        public SummaryWindow next() {
            SummaryWindow ret = nextWindow;
            rocksIterator.next();
            nextWindow = readFromRocksIterator();
            return ret;
        }
    }

    @Override
    Stream<SummaryWindow> getSummaryWindowsOverlapping(long streamID, long t0, long t1, SerDe serde)
            throws BackingStoreException {
        try {
            Iterator<SummaryWindow> iterator = new OverlappingRocksIterator(streamID, t0, t1, serde);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
        } catch (RocksDBException e) {
            throw new BackingStoreException(e);
        }
    }

    @Override
    long getNumSummaryWindows(long streamID, SerDe serde) {
        try (RocksIterator iter = rocksDB.newIterator()){
            iter.seek(getRocksDBKey(streamID, 0L));
            long ct = 0;
            for (; iter.isValid() && getStreamIDFromRocksDBKey(iter.key()) == streamID; iter.next()) {
                ++ct;
            }
            return ct;
        }
    }

    @Override
    public void flushToDisk(long streamID, SerDe serDe) throws BackingStoreException {
        flushLandmarksToDisk(streamID, serDe);
        if (cache == null) return;
        Map<Long, SummaryWindow> streamCache = cache.get(streamID);
        if (streamCache != null) {
            for (Map.Entry<Long, SummaryWindow> entry: streamCache.entrySet()) {
                long swid = entry.getKey();
                SummaryWindow window = entry.getValue();
                byte[] rocksKey = getRocksDBKey(streamID, swid);
                byte[] rocksValue = serDe.serializeSummaryWindow(window);
                try {
                    rocksDB.put(rocksKey, rocksValue);
                } catch (RocksDBException e) {
                    throw new BackingStoreException(e);
                }
            }
        }
    }

    /* **** FIXME: Landmark cache has unbounded size **** */

    private static final int LANDMARK_KEY_SIZE = 17;

    private static byte[] getLandmarkRocksKey(long streamID, long lwid) {
        byte[] keyArray = new byte[LANDMARK_KEY_SIZE];
        keyArray[0] = 'L';
        Utilities.longToByteArray(streamID, keyArray, 1);
        Utilities.longToByteArray(lwid, keyArray, 9);
        return keyArray;
    }

    private ConcurrentHashMap<Long, ConcurrentHashMap<Long, LandmarkWindow>> landmarkCache = new ConcurrentHashMap<>();

    private void flushLandmarksToDisk(long streamID, SerDe serDe) throws BackingStoreException {
        Map<Long, LandmarkWindow> streamMap = landmarkCache.get(streamID);
        if (streamMap == null) return;
        for (Map.Entry<Long, LandmarkWindow> windowEntry: streamMap.entrySet()) {
            long lwid = windowEntry.getKey();
            LandmarkWindow window = windowEntry.getValue();
            try {
                rocksDB.put(getLandmarkRocksKey(streamID, lwid), serDe.serializeLandmarkWindow(window));
            } catch (RocksDBException e) {
                throw new BackingStoreException(e);
            }
        }
    }

    @Override
    LandmarkWindow getLandmarkWindow(long streamID, long lwid, SerDe serDe) throws BackingStoreException {
        Map<Long, LandmarkWindow> streamMap = landmarkCache.get(streamID);
        if (streamMap != null && streamMap.containsKey(lwid)) {
            return streamMap.get(lwid);
        } else {
            byte[] bytes;
            try {
                bytes = rocksDB.get(getLandmarkRocksKey(streamID, lwid));
            } catch (RocksDBException e) {
                throw new BackingStoreException(e);
            }
            return serDe.deserializeLandmarkWindow(bytes);
        }
    }

    @Override
    void putLandmarkWindow(long streamID, long lwid, SerDe serDe, LandmarkWindow window) {
        ConcurrentHashMap<Long, LandmarkWindow> stream = landmarkCache.get(streamID);
        if (stream == null) landmarkCache.put(streamID, (stream = new ConcurrentHashMap<>()));
        stream.put(lwid, window);
    }

    private static final int AUX_KEY_MIN_SIZE = 18;

    private static byte[] getAuxRocksKey(String auxKey) {
        // "AUX" + enough zeroes to fill AUX_KEY_MIN_SIZE + auxKey
        byte[] auxKeyBytes = auxKey.getBytes();
        byte[] key = new byte[AUX_KEY_MIN_SIZE + auxKeyBytes.length];
        key[0] = 'A'; key[1] = 'U'; key[2] = 'X';
        System.arraycopy(auxKeyBytes, 0, key, AUX_KEY_MIN_SIZE, auxKeyBytes.length);
        return key;
    }

    @Override
    public byte[] getAux(String key) throws BackingStoreException {
        try {
            return rocksDB.get(getAuxRocksKey(key));
        } catch (RocksDBException e) {
            throw new BackingStoreException(e);
        }
    }

    @Override
    public void putAux(String key, byte[] value) throws BackingStoreException {
        try {
            rocksDB.put(getAuxRocksKey(key), value);
        } catch (RocksDBException e) {
            throw new BackingStoreException(e);
        }
    }

    @Override
    public void close() throws BackingStoreException {
        if (rocksDB != null) rocksDB.close();
        rocksDBOptions.close();
        logger.info("rocksDB closed");
    }
}
