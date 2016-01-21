package com.samsung.sra.TimeDecayedStore;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by a.vulimiri on 1/20/16.
 */
public class Bucket implements Serializable {
    int count = 0;
    int sum = 0;
    final BucketInfo info;

    public Bucket(BucketInfo info) {
        this.info = info;
    }

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
                assert n > info.endN;
                info.endN = n;
            }
        }
    }
}
