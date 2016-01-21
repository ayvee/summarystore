package com.samsung.sra.TimeDecayedStore;

import java.util.*;

/**
 * Implements WBMH with pow(base) exponential window sizes.
 * Created by a.vulimiri on 1/20/16.
 */
public class WBMHBucketMerger implements BucketMerger {
    private final int base;

    public WBMHBucketMerger(int base) {
        this.base = base;
    }

    /**
     * Is the interval [startN, endN] contained within the same window at the point in time
     * when n elements have been inserted?
     */
    private boolean checkIfSameWindow(int startN, int endN, int n) {
        /* Equivalent to: is there an i such that
                (pow(base, i) - 1) / (base - 1) + 1 <= n - endN <= n - startN <= (pow(base, i+1) - 1) / (base - 1)
         */
        assert startN <= endN;
        if (endN > n) {
            return false;
        }
        int l = n - endN, r = n - startN;
        // TODO: can convert to a single check based on logarithms, figure out how
        for (int i = 0; ; ++i) {
            int pow_b_i = 1;
            for (int j = 0; j < i; ++j) {
                pow_b_i *= base;
            }
            int L = (pow_b_i - 1) / (base - 1) + 1;
            int R = (base * pow_b_i - 1) / (base - 1);
            if (R < l) continue; // [L, R] is strictly to the left of [l, r]
            if (L > r) return false; // [L, R] is strictly to the right of [l, r]
            if (L <= l && r <= R) return true; // [l, r] is strictly contained in [L, R]
        }
    }

    public List<List<BucketID>> merge(LinkedHashMap<BucketID, BucketInfo> baseBuckets, int N0, int N) {
        Map<BucketID, TreeSet<BucketID>> merges = new HashMap<BucketID, TreeSet<BucketID>>();
        /* We process batch appends by adding elements one at a time and seeing which buckets
        would be merged at each point. Have not found a more elegant solution. */
        for (int n = N0 + 1; n <= N; ++n) {
            // consider buckets two at a time, checking if prevBucket and currBucket are in the same window
            // (recall that WBMH = merge any buckets that are in the same window)
            BucketInfo prevBucket = null;
            for (Iterator<Map.Entry<BucketID, BucketInfo>> iter = baseBuckets.entrySet().iterator(); iter.hasNext(); ) {
                BucketInfo currBucket = iter.next().getValue();
                if (prevBucket == null) {
                    prevBucket = currBucket;
                    continue;
                }
                if (checkIfSameWindow(prevBucket.startN, currBucket.endN, n)) {
                    // merge bucket into previous bucket
                    addMerge(merges, prevBucket.bucketID, currBucket.bucketID);
                    prevBucket.endN = currBucket.endN;
                    iter.remove();
                } else {
                    prevBucket = currBucket;
                }
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
        Set<BucketID> recursiveMerges = merges.get(src);
        if (recursiveMerges != null) {
            for (BucketID bucketID: recursiveMerges) {
                merges.get(dst).add(bucketID);
            }
        }
    }
}
