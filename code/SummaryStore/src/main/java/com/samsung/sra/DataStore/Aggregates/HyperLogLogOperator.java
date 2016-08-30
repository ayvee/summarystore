package com.samsung.sra.DataStore.Aggregates;

import com.google.protobuf.Message;
import com.samsung.sra.DataStore.Bucket;
import com.samsung.sra.DataStore.ResultError;
import com.samsung.sra.DataStore.StreamStatistics;
import com.samsung.sra.DataStore.WindowOperator;
import com.samsung.sra.protocol.Summarybucket;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/** Implements COUNT(DISTINCT) over the values in an integer stream.
 * TODO: serialization */
public class HyperLogLogOperator implements WindowOperator<HyperLogLog, Integer, Long, Long> {
    private static List<String> supportedQueries = Collections.singletonList("count");

    @Override
    public List<String> getSupportedQueryTypes() {
        return supportedQueries;
    }

    @Override
    public HyperLogLog createEmpty() {
        return new HyperLogLog();
    }

    @Override
    public HyperLogLog merge(Stream<HyperLogLog> aggrs) {
        HyperLogLog mergedResult = new HyperLogLog();
        aggrs.forEach(hll ->
                mergedResult.estimate += hll.estimate);
        return mergedResult;
    }

    @Override
    public HyperLogLog insert(HyperLogLog aggr, long timestamp, Integer val) {
        aggr.insert(val);
        return aggr;
    }

    @Override
    public ResultError<Long, Long> query(StreamStatistics streamStats, long T0, long T1, Stream<Bucket> buckets, Function<Bucket, HyperLogLog> aggregateRetriever, long t0, long t1, Object... params) {
        return new ResultError<>(
                (long)Math.ceil(buckets.map(aggregateRetriever).mapToDouble(HyperLogLog::getEstimate).sum()),
                0L);
    }

    @Override
    public ResultError<Long, Long> getEmptyQueryResult() {
        return new ResultError<>(0L, 0L);
    }



    @Override
    public Message.Builder protofy(HyperLogLog aggr) {
        return  null;
    }

    @Override
    public HyperLogLog deprotofy(Message.Builder builder) {
        return null;
    }
}
