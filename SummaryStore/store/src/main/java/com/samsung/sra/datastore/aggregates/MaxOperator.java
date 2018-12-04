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
package com.samsung.sra.datastore.aggregates;

import com.samsung.sra.datastore.*;
import com.samsung.sra.protocol.OpTypeOuterClass.OpType;
import com.samsung.sra.protocol.SummaryStore.ProtoOperator;
import org.apache.commons.lang3.mutable.MutableLong;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Query along a Long stream returning the Long maximum. query() also returns a boolean true if we are certain of
 * the answer (happens when querying only over landmarks) */
public class MaxOperator implements WindowOperator<Long, Long, Boolean> {
    private static final OpType opType = OpType.MAX;

    /** What value to return for max over empty set */
    private static final long EMPTY_MAX = Long.MIN_VALUE;

    @Override
    public OpType getOpType() {
        return opType;
    }

    @Override
    public Long createEmpty() {
        return EMPTY_MAX;
    }

    @Override
    public Long merge(Stream<Long> aggrs) {
        return aggrs.mapToLong(Long::longValue).max().orElse(EMPTY_MAX);
    }

    @Override
    public Long insert(Long aggr, long timestamp, Object val) {
        return Math.max(aggr, (Long) val);
    }

    @Override
    public ResultError<Long, Boolean> query(StreamStatistics streamStats,
                                         Stream<SummaryWindow> summaryWindows,
                                         Function<SummaryWindow, Long> summaryRetriever,
                                         Stream<LandmarkWindow> landmarkWindows,
                                         long t0, long t1, Object... params) {
        long smax = merge(summaryWindows.map(summaryRetriever));
        MutableLong lmaxM = new MutableLong(EMPTY_MAX);
        landmarkWindows.forEach(w -> w.values.forEach((t, v) -> {
            if (t0 <= t && t <= t1) {
                lmaxM.setValue(Math.max(lmaxM.longValue(), (Long) v));
            }
        }));
        long lmax = lmaxM.longValue();
        return new ResultError<>(Math.max(smax, lmax), smax == EMPTY_MAX);
    }

    @Override
    public ResultError<Long, Boolean> getEmptyQueryResult() {
        return new ResultError<>(EMPTY_MAX, true);
    }

    @Override
    public ProtoOperator.Builder protofy(Long aggr) {
        return ProtoOperator.newBuilder().setLong(aggr);
    }

    @Override
    public Long deprotofy(ProtoOperator operator) {
        return operator.getLong();
    }
}
