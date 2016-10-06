package com.samsung.sra.DataStore.Aggregates;

import com.google.protobuf.Message;
import com.samsung.sra.DataStore.Bucket;
import com.samsung.sra.DataStore.ResultError;
import com.samsung.sra.DataStore.StreamStatistics;
import com.samsung.sra.DataStore.WindowOperator;
import com.samsung.sra.protocol.Summarybucket;
import org.apache.commons.math3.util.Pair;
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
    public Long insert(Long aggr, long ts, Object[] val) {
        return aggr + (long) val[0];
    }

    @Override
    public ResultError<Double, Pair<Double, Double>> query(StreamStatistics streamStats,
                                    long T0, long T1, Stream<Bucket> buckets, Function<Bucket, Long> sumRetriever,
                                    long t0, long t1, Object... params) {
        double S = buckets.map(sumRetriever).mapToLong(Long::longValue).sum();
        double t = (t1 - t0 + 1), T = (T1 - T0 + 1);
        // TODO: error
        return new ResultError<>(S * t / T, null);
    }

    @Override
    public ResultError<Double, Pair<Double, Double>> getEmptyQueryResult() {
        return new ResultError<>(0d, new Pair<>(0d, 0d));
    }

    @Override
    public Message.Builder protofy(Long aggr) {
        return  Summarybucket.ProtoSum.
                newBuilder().
                setSum(aggr);
    }

    @Override
    public Long deprotofy(Message.Builder builder) {
        return ((Summarybucket.ProtoSum.Builder) builder).getSum();
    }
}
