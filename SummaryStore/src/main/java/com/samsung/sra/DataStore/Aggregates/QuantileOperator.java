package com.samsung.sra.DataStore.Aggregates;

import com.samsung.sra.DataStore.Bucket;
import com.samsung.sra.DataStore.ResultError;
import com.samsung.sra.DataStore.StreamStatistics;
import com.samsung.sra.DataStore.WindowOperator;
import com.samsung.sra.protocol.Summarybucket.ProtoOperator;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class QuantileOperator implements WindowOperator<QDigest, Long, Long> {
    private static final List<String> supportedQueries = Collections.singletonList("quantile");

    private long comprFactor = 64;
    private long byteCount = 2048;

    public QuantileOperator(long compFactor) {
        this.comprFactor = compFactor;
    }

    public QuantileOperator() {
        this.comprFactor = 64;
    }

    @Override
    public List<String> getSupportedQueryTypes() {
        return supportedQueries;
    }

    @Override
    public QDigest createEmpty() {
        return new QDigest(this.comprFactor);
    }

    @Override
    public QDigest merge(Stream<QDigest> aggrs) {
	QDigest newQDigest = new QDigest(this.comprFactor);
        for(QDigest digestItem : (Iterable<QDigest>) aggrs::iterator){
            newQDigest = QDigest.unionOf(newQDigest, digestItem);
        }
	return newQDigest;
    }

    @Override
    public QDigest insert(QDigest aggr, long ts, Object... val) {
	    aggr.insert((long)val[0]);
        return aggr;
    }

    @Override
    public ResultError<Long, Long> query(StreamStatistics streamStats,
                                         long T0, long T1, Stream<Bucket> buckets, Function<Bucket, QDigest> quantileRetriever,
                                         long t0, long t1, Object... params) {
        QDigest newQDigest = new QDigest(64);
        int i = 0; 
        for(Bucket bucketItem : (Iterable<Bucket>) buckets::iterator) {
            newQDigest = QDigest.unionOf(newQDigest, quantileRetriever.apply(bucketItem));
            i++;
        }
        //System.out.println("query para: " + (double)params[0]);
        System.out.println("touched window " + i);
        return new ResultError<>(newQDigest.getQuantile((double)params[0]), 0L);

    }

    @Override
    public ResultError<Long, Long> getEmptyQueryResult() {
        return new ResultError<>(0L, 0L);
    }

    @Override
    public ProtoOperator.Builder protofy(QDigest aggr) {
        return null;
    }

    @Override
    public QDigest deprotofy(ProtoOperator protoOperator) {
        return null;
    }

    /*@Override
    public int getBytecount() {
        return (int)byteCount;
    }

    @Override
    public void serialize(QDigest aggr, byte[] array, int startIndex) {
        byte[] tmpArray;    
        tmpArray = QDigest.serialize(aggr);

        for(int i = 0; i < tmpArray.length; i++) {
            array[startIndex+i] = tmpArray[i];
        }
    }

    @Override
    public QDigest deserialize(byte[] array, int startIndex) {
        byte[] tmpArray = Arrays.copyOfRange(array, startIndex, startIndex+getBytecount());
        return QDigest.deserialize(tmpArray);
    }*/
}
