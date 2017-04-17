package com.samsung.sra.DataStore;

import com.google.protobuf.InvalidProtocolBufferException;
import com.samsung.sra.protocol.SummaryStore.ProtoLandmarkWindow;

import java.io.Serializable;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class LandmarkWindow implements Serializable {
    public final long lwid;
    public long tStart, tEnd;
    public final SortedMap<Long, Object[]> values = new TreeMap<>();

    LandmarkWindow(long lwid, long tStart) {
        this.lwid = lwid;
        this.tStart = tStart;
    }

    public void append(long ts, Object[] value) {
        assert ts >= tStart && (values.isEmpty() || values.lastKey() < ts);
        values.put(ts, value);
    }

    public void close(long ts) {
        assert ts >= tStart && (values.isEmpty() || values.lastKey() <= ts);
        tEnd = ts;
    }

    @Override
    public String toString() {
        return String.format("<landmark-window %d, time range [%d:%d], %d values",
                lwid, tStart, tEnd, values.size());
    }

    public byte[] serialize() {
        ProtoLandmarkWindow.Builder builder = ProtoLandmarkWindow.newBuilder()
                .setLwid(lwid)
                .setTStart(tStart)
                .setTEnd(tEnd);
        for (Map.Entry<Long, Object[]> entry: values.entrySet()) {
            builder.addTimestamp(entry.getKey());
            builder.addValue((long) entry.getValue()[0]);
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
        LandmarkWindow window = new LandmarkWindow(proto.getLwid(), proto.getTStart());
        window.tEnd = proto.getTEnd();
        int N = proto.getTimestampCount();
        assert N == proto.getValueCount();
        for (int i = 0; i < N; ++i) {
            window.values.put(proto.getTimestamp(i), new Object[]{proto.getValue(i)});
        }
        return window;
    }
}
