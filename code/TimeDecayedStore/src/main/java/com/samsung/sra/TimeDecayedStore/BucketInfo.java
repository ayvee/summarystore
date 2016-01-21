package com.samsung.sra.TimeDecayedStore;

/**
 * Created by a.vulimiri on 1/20/16.
 */
class BucketInfo {
    final BucketID bucketID;
    int startN, endN;
    final boolean isLandmark;

    BucketInfo(BucketID bucketID, int startN, int endN, boolean isLandmark) {
        this.bucketID = bucketID;
        this.startN = startN;
        this.endN = endN;
        this.isLandmark = isLandmark;
    }

    BucketInfo(BucketID bucketID, boolean isLandmark) {
        this(bucketID, -1, -1, isLandmark);
    }

    BucketInfo(BucketInfo that) {
        this(that.bucketID, that.startN, that.endN, that.isLandmark);
    }
}
