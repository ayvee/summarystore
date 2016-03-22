package com.samsung.sra.DataStore;

import java.util.*;

/**
 * Implements WBMH with pow(base) exponential window sizes.
 * Created by a.vulimiri on 1/20/16.
 */
public class ExponentialWBMHBucketMerger implements BucketMerger {
    private final int base;

    public ExponentialWBMHBucketMerger(int base) {
        this.base = base;
    }

    /**
     * Is the interval [startN, endN] contained within the same window at the point in time
     * when n elements have been inserted?
     */
    private boolean checkIfSameWindow(int startN, int endN, int n) {
        assert 0 <= startN && startN <= endN;
        if (endN >= n) {
            return false;
        }
        /* Equivalent to: is there an i such that
                (pow(base, i) - 1) / (base - 1) + 1 <= n - endN <= n - startN <= (pow(base, i+1) - 1) / (base - 1) */
        int l = n - endN, r = n - startN;
        for (int i = 0; ; ++i) {
            int pow_b_i = 1;
            for (int j = 0; j < i; ++j) {
                pow_b_i *= base;
            }
            int L = (pow_b_i - 1) / (base - 1) + 1;
            int R = (base * pow_b_i - 1) / (base - 1);
            if (R < l) continue; // [L, R] is strictly to the left of [l, r]
            if (L > r) return false; // [L, R] is strictly to the right of [l, r]
            if (L <= l && r <= R) return true; // [l, r] is contained in [L, R]
        }
        /* In theory, this is the same as:
           is there an i such that
                i <= log_b( (base - 1) * (n - endN - 1) + 1 ) < log_b( (base - 1) * (n - startN) + 1 ) <= i + 1
           iff:
                floor(log_b( (base - 1) * (n - endN - 1) + 1 )) == ceil( log_b( (base - 1) * (n - startN) + 1 )) - 1

        But
          return (int)Math.floor(Math.log((base - 1) * (n - endN - 1) + 1) / Math.log(base)) ==
                 (int)Math.ceil(Math.log((base - 1) * (n - startN) + 1) / Math.log(base)) - 1;
        isn't yielding the same merge sequence (e.g. at n = 100k), possibly because of floating point issues. */
    }

    public List<List<BucketID>> merge(LinkedHashMap<BucketID, BucketInfo> baseBuckets, int N0) {
        Map<BucketID, TreeSet<BucketID>> merges = new HashMap<BucketID, TreeSet<BucketID>>();
        // consider buckets two at a time, checking if prevBucket and currBucket are in the same window
        // (recall that WBMH = merge any consecutive buckets that are in the same window)
        BucketInfo prevBucket = null;
        for (Iterator<Map.Entry<BucketID, BucketInfo>> iter = baseBuckets.entrySet().iterator(); iter.hasNext(); ) {
            BucketInfo currBucket = iter.next().getValue();
            if (prevBucket == null) {
                prevBucket = currBucket;
                continue;
            }
            if (checkIfSameWindow(prevBucket.startN, currBucket.endN, N0 + 1)) {
                // merge bucket into previous bucket
                addMerge(merges, prevBucket.bucketID, currBucket.bucketID);
                prevBucket.endN = currBucket.endN;
                iter.remove();
            } else {
                prevBucket = currBucket;
            }
        }
        List<List<BucketID>> ret = new ArrayList<List<BucketID>>();
        for (BucketID bucketID: baseBuckets.keySet()) {
            List<BucketID> ls = new ArrayList<BucketID>();
            ls.add(bucketID);
            if (merges.containsKey(bucketID)) {
                ls.addAll(merges.get(bucketID));
            }
            ret.add(ls);
        }
        return ret;
    }

    private void addMerge(Map<BucketID, TreeSet<BucketID>> merges, BucketID dst, BucketID src) {
        if (!merges.containsKey(dst)) {
            merges.put(dst, new TreeSet<BucketID>());
        }
        // src needs to be merged into dst
        merges.get(dst).add(src);
        // any buckets that would have been merged into src now need to be merged into dst instead
        Set<BucketID> recursiveMerges = merges.remove(src);
        if (recursiveMerges != null) {
            for (BucketID bucketID: recursiveMerges) {
                merges.get(dst).add(bucketID);
            }
        }
    }
}
