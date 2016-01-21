package com.samsung.sra.TimeDecayedStore;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Bucket implements Serializable {
    public static final int QUERY_COUNT = 0, QUERY_SUM = 1;
    int count = 0;
    int sum = 0;
    final BucketInfo info;

    public Bucket(BucketInfo info) { this.info = info; }

    //public Bucket(BucketID bucketID, boolean isALandmark) { this(new BucketInfo(bucketID, isALandmark)); }

    public void merge(List<Bucket> buckets, TreeMap<Integer, Object> values) {
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
                if (info.endN < n)
                    info.endN = n;
                if (info.startN == -1) // this is the first element inserted into the bucket
                    info.startN = n;
            }
        }
    }

    public int query(int queryType, int t0, int t1) throws QueryException {
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
