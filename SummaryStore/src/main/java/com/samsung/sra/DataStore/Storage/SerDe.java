package com.samsung.sra.DataStore.Storage;

import com.google.protobuf.InvalidProtocolBufferException;
import com.samsung.sra.DataStore.LandmarkWindow;
import com.samsung.sra.DataStore.SummaryWindow;
import com.samsung.sra.DataStore.WindowOperator;
import com.samsung.sra.protocol.SummaryStore;

import java.io.Serializable;
import java.util.Map;

class SerDe implements Serializable {
    private final WindowOperator[] operators;

    SerDe(WindowOperator[] operators) {
        this.operators = operators;
    }

    byte[] serializeSummaryWindow(SummaryWindow window) {
        assert window != null;
        SummaryStore.ProtoSummaryWindow.Builder protoWindow = SummaryStore.ProtoSummaryWindow.newBuilder()
                .setTs(window.ts)
                .setTe(window.te)
                .setCs(window.cs)
                .setCe(window.ce);
        for (int op = 0; op < operators.length; ++op) {
            try {
                assert window.aggregates[op] != null;
                protoWindow.addOperator(operators[op].protofy(window.aggregates[op]));
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        return protoWindow.build().toByteArray();
    }

    SummaryWindow deserializeSummaryWindow(byte[] bytes) {
        SummaryStore.ProtoSummaryWindow protoSummaryWindow;
        try {
            protoSummaryWindow = SummaryStore.ProtoSummaryWindow.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        SummaryWindow window = new SummaryWindow();
        window.ts = protoSummaryWindow.getTs();
        window.te = protoSummaryWindow.getTe();
        window.cs = protoSummaryWindow.getCs();
        window.ce = protoSummaryWindow.getCe();

        assert protoSummaryWindow.getOperatorCount() == operators.length;
        window.aggregates = new Object[operators.length];
        for (int op = 0; op < operators.length; ++op) {
            window.aggregates[op] = operators[op].deprotofy(protoSummaryWindow.getOperator(op));
        }

        return window;
    }

    byte[] serializeLandmarkWindow(LandmarkWindow window) {
        SummaryStore.ProtoLandmarkWindow.Builder builder = SummaryStore.ProtoLandmarkWindow.newBuilder()
                .setTs(window.ts)
                .setTe(window.te);
        for (Map.Entry<Long, Object> entry: window.values.entrySet()) {
            builder.addTimestamp(entry.getKey());
            builder.addValue((long) entry.getValue());
        }
        return builder.build().toByteArray();
    }

    LandmarkWindow deserializeLandmarkWindow(byte[] bytes) {
        SummaryStore.ProtoLandmarkWindow proto;
        try {
            proto = SummaryStore.ProtoLandmarkWindow.parseFrom(bytes);
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
