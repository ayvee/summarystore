package com.samsung.sra.DataStore.Aggregates;

import com.samsung.sra.DataStore.Bucket;
import com.samsung.sra.DataStore.ResultError;
import com.samsung.sra.DataStore.Utilities;
import com.samsung.sra.DataStore.WindowOperator;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class SimpleCountOperator implements WindowOperator<Long, Long, Long, Long> {
    private static final List<String> supportedQueries = Collections.singletonList("count");

    public enum Estimator {
        UPPER_BOUND {
            @Override
            long estimate(long qt0, long qt1, long bt0, long bt1, long bCount) {
                return bCount;
            }
        },
        PROPORTIONAL {
            @Override
            long estimate(long qt0, long qt1, long bt0, long bt1, long bCount) {
                // the intersection of [a, b] and [p, q] is [max(a, p), min(b, q)]
                long l = Math.max(qt0, bt0), r = Math.min(qt1, bt1);
                assert r >= l && r - l <= bt1 - bt0;
                return bCount * (r - l + 1) / (bt1 - bt0 + 1); // TODO: check for int overflow issues
            }
        },
        HALF_BOUND {
            @Override
            long estimate(long qt0, long qt1, long bt0, long bt1, long bCount) {
                if (qt0 <= bt0 && bt1 <= qt1) { // perfect alignment, meaning bCount is the true answer
                    return bCount;
                } else {
                    return bCount / 2;
                }
            }
        };

        abstract long estimate(long qt0, long qt1, long bt0, long bt1, long bCount);
    }

    public final Estimator estimator;

    public SimpleCountOperator(Estimator estimator) {
        this.estimator = estimator;
    }

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
        return aggr + 1;
    }

    @Override
    public ResultError<Long, Long> query(Stream<Bucket> buckets, Function<Bucket, Long> countRetriever, long t0, long t1, Object... params) {
        return new ResultError<>(buckets.map(b ->
                estimator.estimate(t0, t1, b.tStart, b.tEnd, countRetriever.apply(b))
        ).mapToLong(Long::longValue).sum(), 0L);
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
        Utilities.longToByteArray(aggr, array, startIndex);
    }

    @Override
    public Long deserialize(byte[] array, int startIndex) {
        return Utilities.byteArrayToLong(array, startIndex);
    }
}
