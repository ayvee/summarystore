package com.samsung.sra.DataStore;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

class Bucket implements Serializable {
    private long count = 0;
    private long sum = 0;
    final BucketMetadata metadata;

    /** Size of the bucket itself, not counting metadata */
    static final int byteCount = 8 + 8;

    Bucket(BucketMetadata metadata) { this.metadata = metadata; }

    void merge(List<Bucket> buckets) {
        if (buckets != null) {
            for (Bucket that : buckets) {
                this.count += that.count;
                this.sum += that.sum;
                assert this.metadata.cStart < that.metadata.cStart && this.metadata.tStart.compareTo(that.metadata.tStart) < 0;
            }
        }
    }

    void insertValue(Timestamp ts, Object value) {
        assert metadata.tStart.compareTo(ts) <= 0;
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
        String ret = "<bucket " + metadata.bucketID;
        ret += ", tStart " + metadata.tStart;
        ret += ", cStart " + metadata.cStart;
        ret += ", count " + count;
        ret += ", sum " + sum;
        ret += ">";
        return ret;
    }
}
