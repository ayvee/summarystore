package com.samsung.sra.DataStore.Aggregates;

import com.samsung.sra.DataStore.Bucket;
import com.samsung.sra.DataStore.ResultError;
import com.samsung.sra.DataStore.WindowOperator;
import org.apache.commons.lang.NotImplementedException;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class SimpleCountOperator implements WindowOperator<Long, Long, Long, Long> {
    private static final List<String> supportedQueries = Collections.singletonList("count");

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
    public Long insert(Long aggr, long ts, Long val) {
        return aggr + val;
    }

    @Override
    public ResultError<Long, Long> query(Stream<Bucket> buckets, Function<Bucket, Long> countRetriever, long t0, long t1, Object... params) {
        //return new ResultError<>(buckets.mapToInt(b -> ((Integer)countRetriever.apply(b)).intValue()).sum(), 0);
        return new ResultError<>(buckets.map(countRetriever).mapToLong(Long::longValue).sum(), 0L);
    }

    @Override
    public ResultError<Long, Long> getEmptyQueryResult() {
        return new ResultError<>(0L, 0L);
    }

    @Override
    public int getBytecount() {
        return 8;
    }

    @Override
    public void serialize(Long aggr, byte[] array, int startIndex) {
        throw new NotImplementedException();
    }

    @Override
    public Long deserialize(byte[] array, int startIndex) {
        throw new NotImplementedException();
    }
}
