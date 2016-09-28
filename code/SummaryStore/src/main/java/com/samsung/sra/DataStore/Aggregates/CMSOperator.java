package com.samsung.sra.DataStore.Aggregates;

import com.clearspring.analytics.stream.frequency.CMSProtofier;
import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.google.protobuf.Message;
import com.samsung.sra.DataStore.Bucket;
import com.samsung.sra.DataStore.ResultError;
import com.samsung.sra.DataStore.StreamStatistics;
import com.samsung.sra.DataStore.WindowOperator;
import com.samsung.sra.protocol.Summarybucket.ProtoCMS;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// TODO: change Long to byte[]. Library already has most of the needed code, just need some wrapper functions
public class CMSOperator implements WindowOperator<CountMinSketch,Long,Long,Double> {
    private static List<String> supportedQueryTypes = Arrays.asList("frequency", "cms");

    @Override
    public List<String> getSupportedQueryTypes() {
        return supportedQueryTypes;
    }

    private int depth, width, seed;

    public CMSOperator(int depth, int width, int seed) {
        this.depth = depth;
        this.width = width;
        this.seed = seed;
    }

    @Override
    public CountMinSketch createEmpty() {
        return new CountMinSketch(depth, width, seed);
    }

    @Override
    public CountMinSketch merge(Stream<CountMinSketch> aggrs) {
        // TODO: modify CMS code to directly take a stream/iterator w/o buffering into an array first
        CountMinSketch[] all = aggrs.collect(Collectors.toList()).toArray(new CountMinSketch[0]);
        try {
            return CountMinSketch.merge(all);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CountMinSketch insert(CountMinSketch aggr, long timestamp, Long val) {
        aggr.add(val, 1);
        return aggr;
    }

    @Override
    public ResultError<Long, Double> query(StreamStatistics streamStats,
                                           long T0, long T1, Stream<Bucket> buckets,
                                           Function<Bucket, CountMinSketch> aggregateRetriever,
                                           long t0, long t1, Object... params) {
        CountMinSketch union = merge(buckets.map(aggregateRetriever));
        // TODO: error estimate
        return new ResultError<>(union.estimateCount((long) params[0]), 0D);
    }

    @Override
    public ResultError<Long, Double> getEmptyQueryResult() {
        return new ResultError<>(0L, 0D);
    }

    /** protofy code needs access to package-local members, so put it in the com.clearspring... package */
    @Override
    public Message.Builder protofy(CountMinSketch aggr) {
        return CMSProtofier.protofy(aggr);
    }

    @Override
    public CountMinSketch deprotofy(Message.Builder builder) {
        return CMSProtofier.deprotofy((ProtoCMS.Builder) builder, depth, width);
    }
}
