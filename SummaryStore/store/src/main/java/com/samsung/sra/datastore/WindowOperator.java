package com.samsung.sra.datastore;

import com.samsung.sra.protocol.OpTypeOuterClass.OpType;
import com.samsung.sra.protocol.SummaryStore.ProtoOperator;

import java.io.Serializable;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Implements all the functions needed to manage aggregate data structures of type A which returns query answers of type
 * ResultError<R, E>. Note that WindowOperators manage aggregate objects, they are not aggregates themselves (i.e. a
 * BloomFilterOperator object only creates, updates and queries Bloom filters; it is not a Bloom filter itself and does
 * not have an internal bit-array). We maintain one WindowOperator instance per operator per stream.
 *
 * If it helps think about things, this interface was originally called AggregateManager.
 */
public interface WindowOperator<A, R, E> extends Serializable {
    /** Return operator type, e.g. OpType.MAX */
    OpType getOpType();

    /** Create an empty aggregate (containing zero elements) */
    A createEmpty();

    /** Union a sequence of aggregates into one */
    A merge(Stream<A> aggrs);

    /** Insert (the potentially multi-dimensional) val into aggr and return the updated aggregate */
    A insert(A aggr, long timestamp, Object val);

    /** Retrieve aggregates from a set of windows spanning [T0, T1] and do a combined query over
     * them. We pass full SummaryWindow objects instead of specific Aggregate objects of type A to allow
     * query() to access SummaryWindow metadata.
     */
    ResultError<R, E> query(StreamStatistics streamStats,
                            Stream<SummaryWindow> summaryWindows, Function<SummaryWindow, A> summaryRetriever,
                            Stream<LandmarkWindow> landmarkWindows,
                            long t0, long t1, Object... params);

    /** Return the default answer to a query on an empty aggregate (containing zero elements) */
    ResultError<R, E> getEmptyQueryResult();

    ProtoOperator.Builder protofy(A aggr);

    A deprotofy(ProtoOperator protoOperator);
}
