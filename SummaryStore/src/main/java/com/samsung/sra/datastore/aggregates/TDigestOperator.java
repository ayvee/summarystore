package com.samsung.sra.datastore.aggregates;

import com.clearspring.analytics.stream.quantile.TDigest;
import com.google.protobuf.ByteString;
import com.samsung.sra.datastore.*;
import com.samsung.sra.protocol.SummaryStore.ProtoOperator;
import org.apache.commons.math3.util.Pair;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/* Consider evaluating and comparing against http://dl.acm.org/citation.cfm?id=1756034 */
public class TDigestOperator implements WindowOperator<TDigest, Double, Pair<Double, Double>> {
    private static final List<String> supportedQueries = Arrays.asList("quantile", "frequency");

    private final double compression;

    public TDigestOperator(long nbytes) {
        assert nbytes > 76;
        this.compression = (nbytes - 16) / 60; // see TDigest.size()
    }

    @Override
    public List<String> getSupportedQueryTypes() {
        return supportedQueries;
    }

    @Override
    public TDigest createEmpty() {
        return new TDigest(compression);
    }

    @Override
    public TDigest merge(Stream<TDigest> aggrs) {
        return TDigest.merge(compression, aggrs::iterator);
    }

    @Override
    public TDigest insert(TDigest aggr, long timestamp, Object val) {
        /*double value = ((Number) val[0]).doubleValue();
        if (val.length == 1) {
            aggr.add(value);
        } else {
            int weight = ((Number) val[1]).intValue();
            aggr.add(value, weight);
        }*/
        aggr.add(((Number) val).doubleValue());
        return aggr;
    }

    /**
     * Hacky, but:
     *   if passed one param, interpret query as quantile(params[0])
     *   if passed 2/3 params, interpret query as frequency(params[0], params[1][, confidenceLevel])
     */
    @Override
    public ResultError<Double, Pair<Double, Double>> query(StreamStatistics streamStats,
                                                           Stream<SummaryWindow> summaryWindows,
                                                           Function<SummaryWindow, TDigest> tdigestRetriever,
                                                           Stream<LandmarkWindow> landmarkWindows,
                                                           long t0, long t1, Object... params) {
        if (params == null) throw new IllegalArgumentException("TDigest expects non-null params");
        if (params.length == 1) {
            double q = ((Number) params[0]).doubleValue();
            return quantileQuery(streamStats, summaryWindows, tdigestRetriever, landmarkWindows, t0, t1, q);
        } else if (params.length == 2 || params.length == 3) {
            double l = ((Number) params[0]).doubleValue();
            double r = ((Number) params[1]).doubleValue();
            double confidenceLevel = 1;
            if (params.length == 3) {
                confidenceLevel = ((Number) params[2]).doubleValue();
            }
            return frequencyQuery(streamStats, summaryWindows, tdigestRetriever, landmarkWindows, t0, t1, l, r,
                    confidenceLevel);
        } else {
            throw new IllegalArgumentException("invalid parameter count " + params.length + " in TDigest");
        }
    }

    /** [t0, t1] freq[l, r] */
    private ResultError<Double, Pair<Double, Double>> quantileQuery(StreamStatistics streamStats,
                                                                     Stream<SummaryWindow> summaryWindows,
                                                                     Function<SummaryWindow, TDigest> tdigestRetriever,
                                                                     Stream<LandmarkWindow> landmarkWindows,
                                                                     long t0, long t1, double q) {
        TDigest union = merge(summaryWindows.map(tdigestRetriever));
        landmarkWindows.forEach(w -> w.values.forEach((t, v) -> {
            if (t0 <= t && t <= t1) {
                insert(union, t, v);
            }
        }));
        return new ResultError<>(union.quantile(q), null);
    }

    /** [t0, t1] freq[l, r] */
    private ResultError<Double, Pair<Double, Double>> frequencyQuery(StreamStatistics streamStats,
                                                                    Stream<SummaryWindow> summaryWindows,
                                                                    Function<SummaryWindow, TDigest> tdigestRetriever,
                                                                    Stream<LandmarkWindow> landmarkWindows,
                                                                    long t0, long t1, double l, double r,
                                                                    double confidenceLevel) {
        Function<SummaryWindow, Long> countRetriever = tdigestRetriever.andThen(tdigest ->
                Math.round((tdigest.cdf(r) - tdigest.cdf(l)) * tdigest.size())
        );
        Function<Object, Long> landmarkValueParser = o -> {
            double v = ((Number) o).doubleValue();
            return (l <= v && v <= r) ? 1L : 0L;
        };
        double sdMultiplier = streamStats.getCVInterarrival();
        return new SumEstimator(t0, t1, summaryWindows, countRetriever, landmarkWindows, landmarkValueParser)
                .estimate(sdMultiplier, confidenceLevel);
    }

    @Override
    public ResultError<Double, Pair<Double, Double>> getEmptyQueryResult() {
        return new ResultError<>(0d, null);
    }

    @Override
    public ProtoOperator.Builder protofy(TDigest aggr) {
        ByteBuffer buf = ByteBuffer.allocate(aggr.smallByteSize());
        aggr.asSmallBytes(buf);
        return ProtoOperator.newBuilder().setBytearray(ByteString.copyFrom(buf.array()));
    }

    @Override
    public TDigest deprotofy(ProtoOperator protoOperator) {
        byte[] bytes = protoOperator.getBytearray().toByteArray();
        return TDigest.fromBytes(ByteBuffer.wrap(bytes));
    }
}
