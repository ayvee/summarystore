package com.samsung.sra.datastore.aggregates;

import com.samsung.sra.datastore.*;
import com.samsung.sra.protocol.SummaryStore.ProtoOperator;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class SumOperator implements WindowOperator<Long,Double,Pair<Double,Double>> {
    private static Logger logger = LoggerFactory.getLogger(SumOperator.class);

    private static final List<String> supportedQueries = Collections.singletonList("sum");

    @Override
    public List<String> getSupportedQueryTypes() {
        return supportedQueries;
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
        return aggr + (Long) val;
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

        return new SumEstimator(t0, t1, summaryWindows, sumRetriever, landmarkWindows, o -> (Long) o)
                .estimate(sdMultiplier, confidenceLevel);
    }

    @Override
    public ResultError<Double, Pair<Double, Double>> getEmptyQueryResult() {
        return new ResultError<>(0d, new Pair<>(0d, 0d));
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
