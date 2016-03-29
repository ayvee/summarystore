package com.samsung.sra.DataStore;

import java.io.Serializable;

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

    @Override
    public String toString() {
        return "<bucket " + bucketID + " [" + startN + ", " + endN + "] " + (isLandmark ? "landmark" : "non-landmark") + ">";
    }
}
