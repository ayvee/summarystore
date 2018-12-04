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

import com.google.protobuf.InvalidProtocolBufferException;
import com.samsung.sra.datastore.LandmarkWindow;
import com.samsung.sra.datastore.SummaryWindow;
import com.samsung.sra.datastore.WindowOperator;
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
