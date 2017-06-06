package com.samsung.sra.DataStore.Aggregates;

import com.samsung.sra.DataStore.*;
import com.samsung.sra.protocol.Common;
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

/** TODO?: return confidence level along with each CI? */
public class SumOperator implements WindowOperator<Long,Double,Pair<Double,Double>> {
    private static Logger logger = LoggerFactory.getLogger(SumOperator.class);

    private static final List<String> supportedQueries = Collections.singletonList("sum");

    private static final OpType opType = OpType.SUM;

    @Override
    public List<String> getSupportedQueryTypes() {
        return supportedQueries;
    }

    @Override
    public Common.OpType getOpType() {
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
    public Long insert(Long aggr, long ts, Object[] val) {
        return aggr + (long) val[0];
    }

    @Override
    public ResultError<Double, Pair<Double, Double>> query(StreamStatistics streamStats,
                                                           Stream<SummaryWindow> summaryWindows,
                                                           Function<SummaryWindow, Long> sumRetriever,
                                                           Stream<LandmarkWindow> landmarkWindows,
                                                           long t0, long t1, Object... params) {
        double confidenceLevel = 1;
        if (params != null && params.length > 0) {
            confidenceLevel = ((Number) params[0]).doubleValue();
        }
        double cv_t = streamStats.getCVInterarrival(), cv_v = streamStats.getCVValue();
        double mu_v = streamStats.getMeanValue();
        double sdMultiplier = Math.sqrt((cv_t * cv_t + cv_v * cv_v) * mu_v);
            /*double cv_t = streamStats.getCVInterarrival(), cv_v = streamStats.getCVValue();
            double mu_v = streamStats.getMeanValue();
            double sd = Math.sqrt((cv_t * cv_t + cv_v * cv_v) * Math.sqrt(mu_v * var.toDouble()));
            logger.trace("cv_t = {}, cv_v = {}, mu_v = {}, var = {}, sd = {}",
                    cv_t, cv_v, mu_v, var, sd);*/

        return new SumEstimator(t0, t1, summaryWindows, sumRetriever, landmarkWindows, o -> (long) o[0])
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
