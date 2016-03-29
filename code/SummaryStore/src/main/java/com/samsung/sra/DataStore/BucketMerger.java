package com.samsung.sra.DataStore;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * Encapsulates code for EH/WBMH/similar mechanisms
 * Created by a.vulimiri on 1/20/16.
 */
interface BucketMerger {
    /**
     * Figure out what buckets need to be merged after a new element has been inserted.  Specifically,
     * given a list of base buckets spanning the time range [0, N0 + 1), sorted by age oldest bucket first,
     * such that the prefix of buckets spanning [0, N0) were the output of a previous merge operation,
     * return a list of buckets to be merged. Each element in the return list must be a contiguous list
     * of base buckets (sorted oldest first) to be merged. The output must be a chunking of the input
     * list, returned in the same order as the input, e.g.
     *      input = [b0, b1, b2, b3, b4, b5]
     *      output = [[b0, b1, b2], [b3, b4], [b5]]
     *
     * The baseBuckets argument can be consumed destructively.
     */
    List<List<BucketID>> merge(LinkedHashMap<BucketID, BucketInfo> baseBuckets, int N0);
}
