package com.samsung.sra.TimeDecayedStore;

import java.util.*;

/**
 * Created by a.vulimiri on 1/20/16.
 */
public class FixedSizeBucketMerger implements BucketMerger {
    private final int bucketSize;

    public FixedSizeBucketMerger(int bucketSize) {
        this.bucketSize = bucketSize;
    }

    public List<List<BucketID>> merge(LinkedHashMap<BucketID, BucketInfo> baseBuckets, int N0, int N) {
        List<List<BucketID>> ret = new ArrayList<List<BucketID>>();
        int bucketNum = -1, numBuckets = baseBuckets.size();
        for (Iterator<Map.Entry<BucketID, BucketInfo>> iter = baseBuckets.entrySet().iterator(); iter.hasNext(); ) {
            bucketNum += 1;
            Map.Entry<BucketID, BucketInfo> entry = iter.next();
            BucketID bucketID = entry.getKey();
            BucketInfo bucketInfo = entry.getValue();
            ArrayList<BucketID> mergeList = new ArrayList<BucketID>();
            mergeList.add(bucketID);
            if (bucketInfo.endN == bucketInfo.startN && bucketNum <= numBuckets - bucketSize) {
                //System.out.println("Merge target " + bucketID);
                for (int i = 0; i < bucketSize - 1; ++i) {
                    bucketNum += 1;
                    BucketInfo mergee = iter.next().getValue();
                    //System.out.println("Merge source " + mergee.bucketID);
                    assert mergee.endN == mergee.startN;
                    mergeList.add(mergee.bucketID);
                }
            }
            ret.add(mergeList);
        }
        return ret;
    }
}
