package com.samsung.sra.DataStore;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Implements all the functions needed to manage aggregate data structures of type A,
 * which supports inserting values of type V and returns query answers of type ResultError<R, E>.
 * Note that WindowOperators manage aggregate objects, they are not aggregates themselves
 * (i.e. a BloomFilterOperator object only creates, updates and queries Bloom filters; it is not
 * a Bloom filter itself and does not have an internal bit-array)
 */
public interface WindowOperator<A, V, R, E> extends Serializable {
    List<String> getSupportedQueryTypes();

    /** Create an empty aggregate (containing zero elements) */
    A createEmpty();

    /** Union a sequence of aggregates into one */
    A merge(Stream<A> aggrs);

    /** Insert val into aggr and return the updated aggregate */
    A insert(A aggr, long timestamp, V val);

    /**
     * Optional. Operators are free to ignore the Estimator API and directly implement query()
     *
     * Estimators are meant for use by a fuzzy query cache. Basic idea: first time you see a
     * query, you retrieve a bunch of buckets spanning its time range, then construct a small
     * object (the Estimator) encoding all the relevant info from those buckets. This
     * estimator is capable of answering not just the original query [p, q], but also
     * fuzzed queries [p, q] +/- delta. Fuzzy caches can choose to store estimators instead
     * of just the query answer.
     *
     * See SimpleCountOperator for an example implementation.
     */
    interface Estimator<R, E> {
        ResultError<R, E> estimate(long t0, long t1, Object... params);
    }

    /** Build an estimator for a set of buckets spanning [T0, T1] */
    default Estimator<R, E> buildEstimator(StreamStatistics streamStats,
                                           long T0, long T1, Stream<Bucket> buckets, Function<Bucket, A> aggregateRetriever) {
        return null;
    }

    /** Retrieve aggregates from a set of buckets spanning [T0, T1] and do a combined query over
     * them. We pass full Bucket objects instead of specific Aggregate objects of type A to allow
     * query() to access Bucket metadata.
     * TODO: pass an additional Function<Bucket, BucketMetadata> metadataRetriever as argument,
     *       instead of letting query() manhandle Bucket objects
     */
    default ResultError<R, E> query(StreamStatistics streamStats,
                            long T0, long T1, Stream<Bucket> buckets, Function<Bucket, A> aggregateRetriever,
                            long t0, long t1, Object... params) {
        Estimator<R, E> estimator = buildEstimator(streamStats, T0, T1, buckets, aggregateRetriever);
        if (estimator != null) {
            return estimator.estimate(t0, t1, params);
        } else {
            throw new IllegalStateException("operators must implement at least one of query() and buildEstimator()");
        }
    }

    /** Return the default answer to a query on an empty aggregate (containing zero elements) */
    ResultError<R, E> getEmptyQueryResult();

    // AV: commenting out for now, will bring back if we find a need for dry-run error functions
    //abstract public E getError(Stream<Bucket> buckets, long tStart, long tEnd, Object... params);

    /** Number of bytes needed to serialize the aggregate.
     * FIXME: assuming every operator has a fixed byte-count for now to simplify serialization.
     */
    int getBytecount();

    void serialize(A aggr, byte[] array, int startIndex);

    A deserialize(byte[] array, int startIndex);
}
