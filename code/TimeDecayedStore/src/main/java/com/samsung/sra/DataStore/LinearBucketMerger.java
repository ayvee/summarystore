package com.samsung.sra.DataStore;

import java.util.*;

public class LinearBucketMerger implements BucketMerger {
    private final int B;

    public LinearBucketMerger(int bucketSize) {
        this.B = bucketSize;
    }

    private static int bucketSize(BucketInfo bucketInfo) {
        return bucketInfo.endN - bucketInfo.startN + 1;
    }

    public List<List<BucketID>> merge(LinkedHashMap<BucketID, BucketInfo> baseBuckets, int N0, int N) {
        List<List<BucketID>> ret = new ArrayList<List<BucketID>>();
        List<BucketID> mergeList = null;
        BucketInfo prevBucketInfo = null;
        for (BucketInfo bucketInfo: baseBuckets.values()) {
            assert 1 <= bucketSize(bucketInfo) && bucketSize(bucketInfo) <= B;
            if (bucketSize(bucketInfo) == B) {
                // bucket is already full, do nothing
                assert prevBucketInfo == null;
                ret.add(Collections.singletonList(bucketInfo.bucketID));
            } else { // bucket isn't full, size is < B
                if (prevBucketInfo == null) {
                    prevBucketInfo = bucketInfo;
                    mergeList = new ArrayList<BucketID>();
                    mergeList.add(bucketInfo.bucketID);
                    ret.add(mergeList);
                } else {
                    assert bucketSize(prevBucketInfo) + bucketSize(bucketInfo) <= B; // actually size(bucketInfo) should be 1
                    assert prevBucketInfo.endN + 1 == bucketInfo.startN;
                    prevBucketInfo.endN = bucketInfo.endN;
                    mergeList.add(bucketInfo.bucketID);
                    if (bucketSize(prevBucketInfo) == B) { // bucket is now full, seal it
                        prevBucketInfo = null;
                    }
                }
            }
        }
        /*int bucketNum = -1, numBuckets = baseBuckets.size();
        for (Iterator<Map.Entry<BucketID, BucketInfo>> iter = baseBuckets.entrySet().iterator(); iter.hasNext(); ) {
            bucketNum += 1;
            Map.Entry<BucketID, BucketInfo> entry = iter.next();
            BucketID bucketID = entry.getKey();
            BucketInfo bucketInfo = entry.getValue();
            ArrayList<BucketID> mergeList = new ArrayList<BucketID>();
            mergeList.add(bucketID);
            if (bucketInfo.endN == bucketInfo.startN && bucketNum <= numBuckets - B) {
                //System.out.println("Merge target " + bucketID);
                for (int i = 0; i < B - 1; ++i) {
                    bucketNum += 1;
                    BucketInfo mergee = iter.next().getValue();
                    //System.out.println("Merge source " + mergee.bucketID);
                    assert mergee.endN == mergee.startN;
                    mergeList.add(mergee.bucketID);
                }
            }
            ret.add(mergeList);
        }*/
        return ret;
    }
}
