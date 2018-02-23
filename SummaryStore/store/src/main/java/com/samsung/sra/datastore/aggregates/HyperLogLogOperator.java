package com.samsung.sra.datastore.aggregates;

import com.samsung.sra.datastore.*;
import com.samsung.sra.protocol.SummaryStore.ProtoOperator;
import com.samsung.sra.protocol.OpTypeOuterClass.OpType;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/** Implements COUNT(DISTINCT) over the values in an integer stream */
public class HyperLogLogOperator implements WindowOperator<HyperLogLog, Long, Long> {
    private static List<String> supportedQueries = Collections.singletonList("count");
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
    public HyperLogLog insert(HyperLogLog aggr, long timestamp, Object val) {
        aggr.insert((Integer) val);
        return aggr;
    }

    @Override
    public ResultError<Long, Long> query(StreamStatistics streamStats,
                                         Stream<SummaryWindow> summaryWindows,
                                         Function<SummaryWindow, HyperLogLog> hllRetriever,
                                         Stream<LandmarkWindow> landmarkWindows,
                                         long t0, long t1, Object... params) {
        return new ResultError<>(
                (long)Math.ceil(summaryWindows.map(hllRetriever).mapToDouble(HyperLogLog::getEstimate).sum()),
                null);
    }

    @Override
    public ResultError<Long, Long> getEmptyQueryResult() {
        return new ResultError<>(0L, 0L);
    }



    @Override
    public ProtoOperator.Builder protofy(HyperLogLog aggr) {
        return  null;
    }

    @Override
    public HyperLogLog deprotofy(ProtoOperator protoOperator) {
        return null;
    }
}
