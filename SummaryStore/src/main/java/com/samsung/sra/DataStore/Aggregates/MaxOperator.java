package com.samsung.sra.DataStore.Aggregates;

import com.samsung.sra.DataStore.*;
import com.samsung.sra.protocol.SummaryStore.ProtoOperator;
import org.apache.commons.lang.mutable.MutableLong;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Query along a Long stream returning the Long maximum. query() also returns a boolean true if we are certain of
 * the answer (happens when querying only over landmarks) */
public class MaxOperator implements WindowOperator<Long, Long, Boolean> {
    private static final List<String> supportedQueries = Collections.singletonList("max");
    /** What value to return for max over empty set */
    private static final long EMPTY_MAX = Long.MIN_VALUE;

    @Override
    public List<String> getSupportedQueryTypes() {
        return supportedQueries;
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
    public Long insert(Long aggr, long timestamp, Object[] val) {
        return Math.max(aggr, ((Number) val[0]).longValue());
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
                lmaxM.setValue(Math.max(lmaxM.longValue(), ((Number) v[0]).longValue()));
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
