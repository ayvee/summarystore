package com.samsung.sra.DataStore;

import com.google.protobuf.InvalidProtocolBufferException;
import com.samsung.sra.protocol.Attenuation;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.ToDoubleFunction;

/**
 * Time-decayed sampling store. Insert data using append(), and call resample() to sample data down according to the
 * configured decay function. May want to resample() once every so many appends (instead of after all inserts) to keep
 * storage footprint down.
 *
 * FIXME: right now user has to call resample manually. Should auto-resample every K appends, where K is a param
 */
public class AttenuatedStore implements AutoCloseable {
    private static class StreamInfo {
        final long streamID;
        final ToDoubleFunction<Long> decayFunction;
        final StreamStatistics stats = new StreamStatistics();
        final ReadWriteLock lock = new ReentrantReadWriteLock();

        StreamInfo(long streamID, ToDoubleFunction<Long> decayFunction) {
            this.streamID = streamID;
            this.decayFunction = decayFunction;
        }
    }

    private final RocksDB rocksDB;
    private final Options rocksDBOptions;

    private final ConcurrentHashMap<Long, StreamInfo> streamsInfo = new ConcurrentHashMap<>();

    private final Random random;

    public AttenuatedStore(String filePrefix) throws RocksDBException {
        // TODO: resample every few inserts, instead of manually
        String rocksPath = filePrefix + ".sampleStore";
        rocksDBOptions = new Options().setCreateIfMissing(true);
        rocksDB = RocksDB.open(rocksDBOptions, rocksPath);
        random = new Random(0);
    }

    /**
     * Argument should map element age -> probability element should remain in the sample, where age >= 0.
     *
     * FIXME: currently only decay based on count, should eventually also support decay based on time, taking a flag as
     *        argument indicating which for a given stream
     */
    public void registerStream(long streamID, ToDoubleFunction<Long> decayFunction) throws StreamException, RocksDBException {
        if (streamsInfo.put(streamID, new StreamInfo(streamID, decayFunction)) != null) {
            throw new StreamException("attempting to register streamID " + streamID + " multiple times");
        }
    }

    public void append(long streamID, long timestamp, Object... value) throws StreamException, RocksDBException {
        StreamInfo streamInfo = streamsInfo.get(streamID);
        if (streamInfo == null) {
            throw new StreamException("attempting to append to unregistered stream " + streamID);
        }
        if (timestamp <= streamInfo.stats.getTimeRangeEnd()) {
            throw new IllegalArgumentException("out of order insert in stream " + streamID);
        }
        streamInfo.lock.writeLock().lock();
        try {
            long count = streamInfo.stats.getNumValues();
            streamInfo.stats.append(timestamp, value);
            byte[] rocksKey = getRocksKey(streamID, timestamp);
            byte[] rocksValue = Attenuation.ProtoSample
                    .newBuilder()
                    .setCount(count)
                    .setPKeep(1d)
                    .setValue(((Number) value[0]).longValue())
                    .build()
                    .toByteArray();
            rocksDB.put(rocksKey, rocksValue);
        } finally {
            streamInfo.lock.writeLock().unlock();
        }
    }

    public void resample(long streamID) throws StreamException, RocksDBException {
        StreamInfo streamInfo = streamsInfo.get(streamID);
        if (streamInfo == null) {
            throw new StreamException("attempting to append to unregistered stream " + streamID);
        }
        streamInfo.lock.writeLock().lock();
        long N = streamInfo.stats.getNumValues();
        RocksIterator iter = null;
        try {
            iter = rocksDB.newIterator();
            for (iter.seek(getRocksKey(streamID, 0L)); iter.isValid(); iter.next()) {
                byte[] rocksKey = iter.key();
                if (parseRocksKeyStreamID(rocksKey) != streamID) {
                    break;
                } else {
                    Attenuation.ProtoSample value = Attenuation.ProtoSample.parseFrom(iter.value());
                    long age = N - 1 - value.getCount();
                    assert age >= 0;
                    double newPKeep = streamInfo.decayFunction.applyAsDouble(age);
                    double resampleProb = newPKeep / value.getPKeep();
                    assert resampleProb <= 1 + 1e-6;
                    // FIXME! Check if RocksDB allows update while iterating
                    if (random.nextDouble() <= resampleProb) {
                        // value survived resampling
                        byte[] newRocksValue = value.toBuilder().setPKeep(newPKeep).build().toByteArray();
                        rocksDB.put(rocksKey, newRocksValue);
                    } else {
                        // reject value now
                        rocksDB.remove(rocksKey);
                    }
                }
            }
        } catch (InvalidProtocolBufferException e) {
            throw new StreamException(e);
        } finally {
            streamInfo.lock.writeLock().unlock();
            if (iter != null) {
                iter.dispose();
            }
        }
    }

    /**
     * FIXME! Ignores arguments and assumes a SUM query, doesn't do CIs
     */
    public Object query(long streamID, long t0, long t1, int operatorNumber, Object... queryParams) throws StreamException, QueryException, RocksDBException {
        StreamInfo streamInfo = streamsInfo.get(streamID);
        if (streamInfo == null) {
            throw new StreamException("attempting to query unregistered stream " + streamID);
        }
        streamInfo.lock.readLock().lock();
        try {
            double sum = 0;
            RocksIterator iter = rocksDB.newIterator();
            for (iter.seek(getRocksKey(streamID, t0)); iter.isValid(); iter.next()) {
                byte[] rocksKey = iter.key();
                if (parseRocksKeyStreamID(rocksKey) != streamID || parseRocksKeyTimestamp(rocksKey) > t1) {
                    break;
                }
                Attenuation.ProtoSample value = Attenuation.ProtoSample.parseFrom(iter.value());
                sum += value.getValue() / value.getPKeep();
            }
            return new ResultError<Double, Double>(sum, null);
        } catch (InvalidProtocolBufferException e) {
            throw new StreamException(e);
        } finally {
            streamInfo.lock.readLock().unlock();
        }
    }

    private static byte[] getRocksKey(long streamID, long timestamp) {
        byte[] key = new byte[16];
        // FIXME? Only works for non-negative timestamps, depends on Utilities.longToByteArray being big-endian
        Utilities.longToByteArray(streamID, key, 0);
        Utilities.longToByteArray(timestamp, key, 8);
        return key;
    }

    private static long parseRocksKeyStreamID(byte[] key) {
        return Utilities.byteArrayToLong(key, 0);
    }

    private static long parseRocksKeyTimestamp(byte[] key) {
        return Utilities.byteArrayToLong(key, 8);
    }

    public StreamStatistics getStreamStatistics(long streamID) throws StreamException {
        StreamInfo streamInfo = streamsInfo.get(streamID);
        if (streamInfo == null) {
            throw new StreamException("attempting to get stats for unregistered stream " + streamID);
        }
        return new StreamStatistics(streamInfo.stats);
    }

    public void flush(long streamID) throws RocksDBException, StreamException {
        resample(streamID);
    }

    void printSample(long streamID) throws RocksDBException, StreamException, InvalidProtocolBufferException {
        StreamInfo streamInfo = streamsInfo.get(streamID);
        if (streamInfo == null) {
            throw new StreamException("attempting to print unregistered stream " + streamID);
        }
        streamInfo.lock.readLock().lock();
        try {
            RocksIterator iter = rocksDB.newIterator();
            for (iter.seek(getRocksKey(streamID, 0)); iter.isValid(); iter.next()) {
                byte[] rocksKey = iter.key();
                if (parseRocksKeyStreamID(rocksKey) != streamID) {
                    break;
                }
                Attenuation.ProtoSample value = Attenuation.ProtoSample.parseFrom(iter.value());
                System.out.printf("t = %d, v = %d, count = %d, pKeep = %f\n",
                        parseRocksKeyTimestamp(rocksKey), value.getValue(), value.getCount(), value.getPKeep());
            }
        } finally {
            streamInfo.lock.readLock().unlock();
        }
    }

    public void close() throws RocksDBException {
        // TODO: lock out new stream creates
        for (Map.Entry<Long, StreamInfo> entry: streamsInfo.entrySet()) {
            try {
                resample(entry.getKey());
            } catch (StreamException e) {
                throw new IllegalStateException("saw unreachable stream exception", e);
            }
        }
        if (rocksDB != null) {
            rocksDB.close();
        }
        rocksDBOptions.dispose();
    }

    public static void main(String[] args) throws Exception {
        Runtime.getRuntime().exec(new String[]{"sh", "-c", "rm -rf /tmp/astore*"}).waitFor();
        try (AttenuatedStore store = new AttenuatedStore("/tmp/astore")) {
            long streamID = 0;
            ToDoubleFunction<Long> decay = age -> Math.pow(1d / (1d + age), 0.5);
            store.registerStream(streamID, decay);
            for (long i = 0; i < 1e6; ++i) {
                store.append(streamID, i, i);
            }
            store.resample(streamID);
            store.printSample(streamID);
            System.out.println(store.query(streamID, 1000, 10_000, 0));
        }
    }
}
