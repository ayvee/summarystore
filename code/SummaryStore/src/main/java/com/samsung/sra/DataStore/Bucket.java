package com.samsung.sra.DataStore;

import com.samsung.sra.Misc.HyperLogLogAggrOperator;
import com.samsung.sra.Misc.SimpleCount;
import com.samsung.sra.Misc.SimpleSum;

import java.io.Serializable;
import java.util.stream.Stream;

public class Bucket implements Serializable {
    // data
    long count;
    long sum;

    // metadata
    /* We use longs for bucket IDs, timestamps, and count markers. Valid values should be
       non-negative (all three are 0-indexed), and use "-1" to indicate null values. */
    long prevBucketID, thisBucketID, nextBucketID;
    long tStart, tEnd;
    long cStart, cEnd;

    static final int BYTE_COUNT = 8 * 9;

    Bucket(long count, long sum,
           long prevBucketID, long thisBucketID, long nextBucketID,
           long tStart, long tEnd, long cStart, long cEnd) {
        this.count = count;
        this.sum = sum;
        this.prevBucketID = prevBucketID;
        this.thisBucketID = thisBucketID;
        this.nextBucketID = nextBucketID;
        this.tStart = tStart;
        this.tEnd = tEnd;
        this.cStart = cStart;
        this.cEnd = cEnd;
    }

    Bucket(long prevBucketID, long thisBucketID, long nextBucketID,
           long tStart, long tEnd, long cStart, long cEnd) {
        this(0, 0, prevBucketID, thisBucketID, nextBucketID,tStart, tEnd, cStart, cEnd);
    }

    void merge(Bucket... buckets) {
        for (Bucket that: buckets) {
            assert this.cEnd == that.cStart - 1;
            this.count += that.count;
            this.sum += that.sum;
            this.cEnd = that.cEnd;
            this.tEnd = that.tEnd;
        }
    }

    void insertValue(long ts, Object value) {
        assert tStart <= ts && (tEnd == -1 || ts <= tEnd);
        count += 1;
        sum += (long)value;
    }

    static long multiQuery(Stream<Bucket> buckets, long t0, long t1, QueryType queryType, Object[] queryParams) {
        switch (queryType) {
            case COUNT:
                return buckets.mapToLong(b -> b.count).sum();
            case SUM:
                return buckets.mapToLong(b -> b.sum).sum();
            default:
                throw new UnsupportedOperationException("not yet implemented");
        }
    }

    @Override
    public String toString() {
        String ret = "<bucket " + thisBucketID;
        ret += ", time range [" + tStart + ":" + tEnd + "]";
        ret += ", count range [" + cStart + ":" + cEnd + "]";
        ret += ", count " + count;
        ret += ", sum " + sum;
        ret += ">";
        return ret;
    }
}
