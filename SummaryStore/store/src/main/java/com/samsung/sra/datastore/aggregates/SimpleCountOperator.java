package com.samsung.sra.datastore.aggregates;

import com.samsung.sra.datastore.*;
import com.samsung.sra.protocol.Common.OpType;
import com.samsung.sra.protocol.SummaryStore.ProtoOperator;
//import org.apache.commons.math3.util.Pair;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class SimpleCountOperator implements WindowOperator<Long,Double,Pair<Double,Double>> {
    private static Logger logger = LoggerFactory.getLogger(SimpleCountOperator.class);

    private static final List<String> supportedQueries = Collections.singletonList("count");

    private static final OpType opType = OpType.COUNT;

    @Override
    public List<String> getSupportedQueryTypes() {
        return supportedQueries;
    }

    @Override
    public OpType getOpType() {
        return opType;
    }

    @Override
    public Long createEmpty() {
        return 0L;
    }

    @Override
    public Long merge(Stream<Long> aggrs) {
        return aggrs.mapToLong(Long::longValue).sum();
    }

    @Override
    public Long insert(Long aggr, long ts, Object val) {
        return aggr + 1;
    }

    @Override
    public ResultError<Double, Pair<Double, Double>> query(StreamStatistics streamStats,
                                                           Stream<SummaryWindow> summaryWindows,
                                                           Function<SummaryWindow, Long> countRetriever,
                                                           Stream<LandmarkWindow> landmarkWindows,
                                                           long t0, long t1, Object... params) {
        double confidenceLevel = 1;
        if (params != null && params.length > 0) {
            confidenceLevel = ((Number) params[0]).doubleValue();
        }
        //double sd = streamStats.getCVInterarrival() * Math.sqrt(var.toDouble());



        double sdMultiplier = streamStats.getCVInterarrival();
        return new SumEstimator(t0, t1, summaryWindows, countRetriever, landmarkWindows, o -> 1L)
                .estimate(sdMultiplier, confidenceLevel);
    }

    @Override
    public ResultError<Double, Pair<Double, Double>> getEmptyQueryResult() {
        return new ResultError<>(0d, new ImmutablePair<>(0d, 0d));

    }

    @Override
    public ProtoOperator.Builder protofy(Long aggr) {
        return ProtoOperator
                .newBuilder()
                .setLong(aggr);
    }

    @Override
    public Long deprotofy(ProtoOperator operator) {
        return operator.getLong();
    }
}
