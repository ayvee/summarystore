package com.samsung.sra.DataStore;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

class Bucket implements Serializable {
    // data
    private long count = 0;
    private long sum = 0;

    // metadata
    final BucketID id;
    final Timestamp tStart;
    final Long cStart;

    /** Size of the bucket itself, for count and sum, not counting metadata */
    static final int byteCount = Long.BYTES + Long.BYTES;

    Bucket(BucketID bucketID, Timestamp tStart, long cStart) {
        this.id = bucketID;
        this.tStart = tStart;
        this.cStart = cStart;
    }

    void merge(List<Bucket> buckets) {
        if (buckets != null) {
            for (Bucket that : buckets) {
                //TODO: assert this.cEnd == that.cStart - 1;
                this.count += that.count;
                this.sum += that.sum;
            }
        }
    }

    void insertValue(Timestamp ts, Object value) {
        //TODO: assert tStart.compareTo(ts) <= 0 && (tEnd == null || ts.compareTo(tEnd) <= 0);
        count += 1;
        sum += (Long)value;
    }

    long query(Timestamp t0, Timestamp t1, QueryType queryType, Object[] queryParams) throws QueryException {
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
    long multiQuery(Collection<Bucket> rest, Timestamp t0, Timestamp t1, QueryType queryType, Object[] queryParams) throws QueryException {
        long ret = this.query(t0, t1, queryType, queryParams);
        for (Bucket bucket: rest) {
            ret += bucket.query(t0, t1, queryType, queryParams);
        }
        return ret;
    }

    @Override
    public String toString() {
        String ret = "<bucket " + id;
        ret += ", count " + count;
        ret += ", sum " + sum;
        ret += ">";
        return ret;
    }
}
