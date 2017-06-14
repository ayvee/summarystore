package com.samsung.sra.DataStore.Storage;

import com.google.protobuf.InvalidProtocolBufferException;
import com.samsung.sra.DataStore.SummaryWindow;
import com.samsung.sra.DataStore.WindowOperator;
import com.samsung.sra.protocol.SummaryStore;

import java.io.Serializable;

/** Serializer/deserializer for summary windows in a particular stream */
class SerDe implements Serializable {
    private final WindowOperator[] operators;

    SerDe(WindowOperator[] operators) {
        this.operators = operators;
    }

    byte[] serialize(SummaryWindow window) {
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

    SummaryWindow deserialize(byte[] bytes) {
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
}
