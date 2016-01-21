package com.samsung.sra.TimeDecayedStore;

import java.io.Serializable;

/**
 * Created by a.vulimiri on 1/20/16.
 */
class BucketInfo implements Serializable {
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
