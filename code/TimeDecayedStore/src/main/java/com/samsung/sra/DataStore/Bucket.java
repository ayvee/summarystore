package com.samsung.sra.DataStore;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Bucket implements Serializable {
    public static final int QUERY_COUNT = 0, QUERY_SUM = 1;
    private int count = 0;
    private int sum = 0;
    public final BucketInfo info;

    public Bucket(BucketInfo info) { this.info = info; }

    public void merge(List<Bucket> buckets, TreeMap<Integer, Object> values, int finalEndN) {
        if (buckets != null) {
            for (Bucket that: buckets) {
                this.count += that.count;
                this.sum += that.sum;
                assert this.info.endN + 1 == that.info.startN;
                this.info.endN = that.info.endN;
            }
        }
        if (values != null) {
            for (Map.Entry<Integer, Object> entry: values.entrySet()) {
                Integer n = entry.getKey();
                Integer v = (Integer)entry.getValue();
                count += 1;
                sum += v;
                if (n > info.endN) {
                    info.endN = n;
                }
            }
        }
        // finalEndN can be strictly larger if this bucket is "empty" at the end because of a landmark overlap
        assert info.endN <= finalEndN;
        info.endN = finalEndN;
    }

    private int query(int queryType, int t0, int t1) throws QueryException {
        switch (queryType) {
            case QUERY_COUNT:
                return count;
            case QUERY_SUM:
                return sum;
            default:
                throw new QueryException("Invalid query type " + queryType + " in bucket " + info);
        }
    }

    public static int multiBucketQuery(Collection<Bucket> buckets, int queryType, int t0, int t1) throws QueryException {
        int ret = 0;
        for (Bucket bucket: buckets) {
            ret += bucket.query(queryType, t0, t1);
        }
        return ret;
    }
}
