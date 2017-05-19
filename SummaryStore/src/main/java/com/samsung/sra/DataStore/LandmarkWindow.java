package com.samsung.sra.DataStore;

import com.google.protobuf.InvalidProtocolBufferException;
import com.samsung.sra.protocol.SummaryStore.ProtoLandmarkWindow;

import java.io.Serializable;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class LandmarkWindow implements Serializable {
    public long ts, te;
    public final SortedMap<Long, Object> values = new TreeMap<>();

    LandmarkWindow(long ts) {
        this.ts = ts;
    }

    public void append(long timestamp, Object value) {
        assert timestamp >= ts && (values.isEmpty() || values.lastKey() < timestamp);
        values.put(timestamp, value);
    }

    public void close(long timestamp) {
        assert timestamp >= ts && (values.isEmpty() || values.lastKey() <= timestamp);
        te = timestamp;
    }

    @Override
    public String toString() {
        return String.format("<landmark-window: time range [%d:%d], %d values", ts, te, values.size());
    }

    public byte[] serialize() {
        ProtoLandmarkWindow.Builder builder = ProtoLandmarkWindow.newBuilder()
                .setTs(ts)
                .setTe(te);
        for (Map.Entry<Long, Object> entry: values.entrySet()) {
            builder.addTimestamp(entry.getKey());
            builder.addValue((long) entry.getValue());
        }
        return builder.build().toByteArray();
    }

    public static LandmarkWindow deserialize(byte[] bytes) {
        ProtoLandmarkWindow proto;
        try {
             proto = ProtoLandmarkWindow.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        LandmarkWindow window = new LandmarkWindow(proto.getTs());
        window.te = proto.getTe();
        int N = proto.getTimestampCount();
        assert N == proto.getValueCount();
        for (int i = 0; i < N; ++i) {
            window.values.put(proto.getTimestamp(i), proto.getValue(i));
        }
        return window;
    }
}
