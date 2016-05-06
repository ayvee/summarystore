package com.samsung.sra.DataStore;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

class Bucket implements Serializable {
    // data
    private long count = 0;
    private long sum = 0;

    // metadata
    /* We use longs for bucket IDs, timestamps, and count markers. Valid values should be
       non-negative (all three are 0-indexed), and use "-1" to indicate null values. */
    long prevBucketID, thisBucketID, nextBucketID;
    long tStart, tEnd;
    long cStart, cEnd;

    /** Size of the bucket itself, for count and sum, not counting metadata */
    static final int byteCount = 8 + 8;

    Bucket(long prevBucketID, long thisBucketID, long nextBucketID,
           long tStart, long tEnd, long cStart, long cEnd) {
        this.prevBucketID = prevBucketID;
        this.thisBucketID = thisBucketID;
        this.nextBucketID = nextBucketID;
        this.tStart = tStart;
        this.tEnd = tEnd;
        this.cStart = cStart;
        this.cEnd = cEnd;
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

    void merge(List<Bucket> buckets) {
        if (buckets != null) {
            merge(buckets.toArray(new Bucket[buckets.size()]));
        }
    }

    void insertValue(long ts, Object value) {
        assert tStart <= ts && (tEnd == -1 || ts <= tEnd);
        count += 1;
        sum += (long)value;
    }

    long query(long t0, long t1, QueryType queryType, Object[] queryParams) throws QueryException {
        switch (queryType) {
            case COUNT:
                return count;
            case SUM:
                return sum;
            case EXISTENCE:
                throw new UnsupportedOperationException("not yet implemented");
        }
        throw new IllegalStateException("hit unreachable code");
    }

    /**
     * Query a sequence of successive buckets. Sequence = (this bucket) :: rest.
     * The sequence should cover the time range [t0, t1], although we don't sanity check
     * that it does
     */
    long multiQuery(Collection<Bucket> rest, long t0, long t1, QueryType queryType, Object[] queryParams) throws QueryException {
        long ret = this.query(t0, t1, queryType, queryParams);
        for (Bucket bucket: rest) {
            ret += bucket.query(t0, t1, queryType, queryParams);
        }
        return ret;
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
