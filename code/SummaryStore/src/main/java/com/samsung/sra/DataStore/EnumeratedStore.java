package com.samsung.sra.DataStore;

import org.nustaq.serialization.FSTConfiguration;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * Stores all elements explicitly enumerated.
 */
public class EnumeratedStore implements DataStore {
    private RocksDB rocksDB;
    private Options rocksDBOptions;

    private static class StreamInfo implements Serializable {
        final StreamID streamID;
        final Object syncObj = new Object();
        int numValues = 0;
        Timestamp lastValueTimestamp = null;

        StreamInfo(StreamID streamID) {
            this.streamID = streamID;
        }
    }

    private final HashMap<StreamID, StreamInfo> streamsInfo;

    private final static byte[] streamsInfoSpecialKey = {0};

    private void persistStreamsInfo() throws RocksDBException {
        rocksDB.put(streamsInfoSpecialKey, fstConf.asByteArray(streamsInfo));
    }

    public EnumeratedStore(String rocksDBPath) throws RocksDBException {
        rocksDBOptions = new Options().setCreateIfMissing(true);
        rocksDB = RocksDB.open(rocksDBOptions, rocksDBPath);

        byte[] streamCountsBytes = rocksDB.get(streamsInfoSpecialKey);
        streamsInfo = streamCountsBytes != null ?
                (HashMap<StreamID, StreamInfo>) fstConf.asObject(streamCountsBytes) :
                new HashMap<>();
    }

    private static final FSTConfiguration fstConf;

    static {
        fstConf = FSTConfiguration.createDefaultConfiguration();
        fstConf.registerClass(StreamInfo.class);

        RocksDB.loadLibrary();
    }

    public void registerStream(StreamID streamID) throws StreamException, RocksDBException {
        synchronized (streamsInfo) {
            if (streamsInfo.containsKey(streamID)) {
                throw new StreamException("attempting to register stream " + streamID + " twice");
            } else {
                streamsInfo.put(streamID, new StreamInfo(streamID));
                persistStreamsInfo();
            }
        }
    }

    private byte[] getRocksKey(StreamID streamID, Timestamp t) {
        ByteBuffer bytebuf = ByteBuffer.allocate(StreamID.byteCount + Timestamp.byteCount);
        streamID.writeToByteBuffer(bytebuf);
        t.writeToByteBuffer(bytebuf);
        bytebuf.flip();
        return bytebuf.array();
    }

    private Timestamp parseRocksKeyTimestamp(byte[] bytes, StreamID streamID) {
        ByteBuffer bytebuf = ByteBuffer.wrap(bytes);
        StreamID readStreamID = StreamID.readFromByteBuffer(bytebuf);
        assert streamID.equals(readStreamID);
        return Timestamp.readFromByteBuffer(bytebuf);
    }

    private long parseRocksValue(byte[] bytes) throws RocksDBException {
        ByteBuffer bytebuf = ByteBuffer.allocate(8);
        bytebuf.put(bytes);
        bytebuf.flip();
        return bytebuf.getLong();
    }

    private void rocksPut(StreamID streamID, Timestamp t, long value) throws RocksDBException {
        ByteBuffer bytebuf = ByteBuffer.allocate(8);
        bytebuf.putLong(value);
        bytebuf.flip();
        rocksDB.put(getRocksKey(streamID, t), bytebuf.array());
    }

    public Object query(StreamID streamID,
                        Timestamp t0, Timestamp t1, QueryType queryType, Object[] queryParams)
            throws StreamException, QueryException, RocksDBException {
        if (t0.compareTo(new Timestamp(0)) < 0 || t0.compareTo(t1) > 0) {
            throw new QueryException("[" + t0 + ", " + t1 + "] is not a valid time interval");
        }
        StreamInfo streamInfo;
        synchronized (streamsInfo) {
            streamInfo = streamsInfo.get(streamID);
            if (streamInfo == null) {
                throw new StreamException("querying invalid stream " + streamID);
            }
        }
        long ret = 0;
        synchronized (streamInfo.syncObj) {
            RocksIterator iter = null;
            try {
                iter = rocksDB.newIterator();
                for (iter.seek(getRocksKey(streamID, t0)); iter.isValid(); iter.next()) {
                    Timestamp t = parseRocksKeyTimestamp(iter.key(), streamID);
                    if (t.compareTo(t1) > 0) {
                        break;
                    }
                    long v = parseRocksValue(iter.value());
                    switch (queryType) {
                        case COUNT:
                            ret += 1;
                            break;
                        case SUM:
                            ret += v;
                            break;
                        default:
                            throw new QueryException(("invalid query type " + queryType));
                    }
                }
            } finally {
                if (iter != null) iter.dispose();
            }
        }
        return ret;
    }

    @Override
    public void append(StreamID streamID, Timestamp ts, Object value) throws StreamException, RocksDBException {
        StreamInfo streamInfo;
        synchronized (streamsInfo) {
            streamInfo = streamsInfo.get(streamID);
            if (streamInfo == null) {
                throw new StreamException("querying invalid stream " + streamID);
            }
        }
        synchronized (streamInfo.syncObj) {
            assert streamInfo.lastValueTimestamp == null || streamInfo.lastValueTimestamp.compareTo(ts) < 0;
            streamInfo.lastValueTimestamp = ts;
            ++streamInfo.numValues;
            rocksPut(streamID, ts, (Long)value);
        }
        synchronized (streamsInfo) {
            persistStreamsInfo();
        }
    }

    public void close() {
        // FIXME: should wait for any processing appends to terminate first
        if (rocksDB != null) {
            rocksDB.close();
        }
        rocksDBOptions.dispose();
    }

    public long getStoreSizeInBytes() {
        // TODO: synchronize
        long ret = 0;
        for (StreamInfo streamInfo: streamsInfo.values()) {
            ret += streamInfo.numValues * (StreamID.byteCount + Timestamp.byteCount + 8);
        }
        return ret;
    }

    public static void main(String[] args) {
        DataStore store = null;
        try {
            String storeLoc = "/tmp/tdstore";
            // FIXME: add a deleteStream/resetDatabase operation
            Runtime.getRuntime().exec(new String[]{"rm", "-rf", storeLoc}).waitFor();
            store = new EnumeratedStore(storeLoc);
            StreamID streamID = new StreamID(0);
            store.registerStream(streamID);
            for (long i = 0; i < 10; ++i) {
                store.append(streamID, new Timestamp(i), i + 1);
            }
            Timestamp t0 = new Timestamp(0), t1 = new Timestamp(3);
            System.out.println(
                    "sum[" + t0 + ", " + t1 + "] = " + store.query(streamID, t0, t1, QueryType.SUM, null) + "; " +
                            "count[" + t0 + ", " + t1 + "] = " + store.query(streamID, t0, t1, QueryType.COUNT, null));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (store != null) {
                store.close();
            }
        }
    }
}
