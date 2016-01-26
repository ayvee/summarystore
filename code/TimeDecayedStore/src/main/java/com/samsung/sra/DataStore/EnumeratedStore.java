package com.samsung.sra.DataStore;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stores all elements explicitly enumerated.
 */
public class EnumeratedStore implements DataStore {
    public EnumeratedStore(String rocksDBPath) throws RocksDBException {
        // TODO: not yet persisent, we start the DB from scratch on each code run. Should be an easy fix
        rocksDBOptions = new Options().setCreateIfMissing(true);
        rocksDB = RocksDB.open(rocksDBOptions, rocksDBPath);

        streamCounts = new ConcurrentHashMap<StreamID, Integer>();
        streamSyncObjects = new HashMap<StreamID, Object>();
    }

    static {
        RocksDB.loadLibrary();
    }

    private RocksDB rocksDB;
    private Options rocksDBOptions;
    private final ConcurrentHashMap<StreamID, Integer> streamCounts;
    private final Map<StreamID, Object> streamSyncObjects;

    public void registerStream(StreamID streamID) throws StreamException {
        synchronized (streamCounts) {
            if (streamCounts.containsKey(streamID)) {
                throw new StreamException("attempting to register stream " + streamID + " twice");
            } else {
                streamCounts.put(streamID, 0);
                streamSyncObjects.put(streamID, new Object());
            }
        }
    }

    private byte[] getRocksKey(StreamID streamID, int t) {
        ByteBuffer bytebuf = ByteBuffer.allocate(StreamID.byteCount + 4);
        streamID.writeToByteBuffer(bytebuf);
        bytebuf.putInt(t);
        bytebuf.flip();
        return bytebuf.array();
    }

    private int rocksGet(StreamID streamID, int t) throws RocksDBException {
        byte[] bytes = rocksDB.get(getRocksKey(streamID, t));
        ByteBuffer bytebuf = ByteBuffer.allocate(4);
        bytebuf.put(bytes);
        bytebuf.flip();
        return bytebuf.getInt();
    }

    private void rocksPut(StreamID streamID, int t, int value) throws RocksDBException {
        ByteBuffer bytebuf = ByteBuffer.allocate(4);
        bytebuf.putInt(value);
        bytebuf.flip();
        rocksDB.put(getRocksKey(streamID, t), bytebuf.array());
    }

    public Object query(StreamID streamID, int queryType, int t0, int t1) throws StreamException, QueryException, RocksDBException {
        if (t0 < 0 || t0 > t1) {
            throw new QueryException("[" + t0 + ", " + t1 + "] is not a valid time interval");
        }
        Object syncobj;
        synchronized (streamCounts) {
            if (!streamSyncObjects.containsKey(streamID)) {
                throw new StreamException("querying invalid stream " + streamID);
            }
            syncobj = streamSyncObjects.get(streamID);
        }
        int ret = 0;
        synchronized (syncobj) {
            if (t1 >= streamCounts.get(streamID)) {
                throw new QueryException("[" + t0 + ", " + t1 + "] is not a valid time interval");
            }
            for (int t = t0; t <= t1; ++t) {
                int v = rocksGet(streamID, t);
                switch (queryType) {
                    case Bucket.QUERY_COUNT:
                        ret += 1;
                        break;
                    case Bucket.QUERY_SUM:
                        ret += v;
                        break;
                    default:
                        throw new QueryException(("invalid query type " + queryType));
                }
            }
        }
        return ret;
    }

    public void append(StreamID streamID, Collection<FlaggedValue> values) throws StreamException, LandmarkEventException, RocksDBException {
        if (values == null || values.isEmpty()) {
            return;
        }
        Object syncobj;
        synchronized (streamCounts) {
            if (!streamSyncObjects.containsKey(streamID)) {
                throw new StreamException("querying invalid stream " + streamID);
            }
            syncobj = streamSyncObjects.get(streamID);
        }
        synchronized (syncobj) {
            int t0 = streamCounts.get(streamID), t = t0 - 1;
            for (FlaggedValue fv: values) {
                ++t;
                rocksPut(streamID, t, (Integer)fv.value);
            }
            streamCounts.put(streamID, t0 + values.size());
        }
    }

    public void close() {
        // FIXME: should wait for any processing appends to terminate first
        if (rocksDB != null) {
            rocksDB.close();
        }
        rocksDBOptions.dispose();
    }

    public static void main(String[] args) {
        DataStore store = null;
        try {
            store = new EnumeratedStore("/tmp/tdstore");
            StreamID streamID = new StreamID(0);
            store.registerStream(streamID);
            for (int i = 0; i < 10; ++i) {
                List<FlaggedValue> values = new ArrayList<FlaggedValue>();
                values.add(new FlaggedValue(i+1));
                if (i == 4) values.get(0).landmarkStartsHere = true;
                if (i == 6) values.get(0).landmarkEndsHere = true;
                store.append(streamID, values);
            }
            int t0 = 0, t1 = 9;
            System.out.println(
                    "sum[" + t0 + ", " + t1 + "] = " + store.query(streamID, Bucket.QUERY_SUM, t0, t1) + "; " +
                            "count[" + t0 + ", " + t1 + "] = " + store.query(streamID, Bucket.QUERY_COUNT, t0, t1));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (store != null) {
                store.close();
            }
        }
    }
}
