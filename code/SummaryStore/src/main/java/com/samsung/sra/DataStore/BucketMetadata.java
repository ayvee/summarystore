package com.samsung.sra.DataStore;

import java.io.Serializable;

class BucketMetadata implements Serializable {
    final BucketID bucketID;
    // This Bucket covers elements in the time range [tStart, nextBucket.tStart), with insert
    // counts [cStart, nextBucket.cStart)
    final Timestamp tStart;
    final long cStart;

    static final int byteCount = BucketID.byteCount + Timestamp.byteCount + 8;

    BucketMetadata(BucketID bucketID, Timestamp tStart, long cStart) {
        this.bucketID = bucketID;
        this.tStart = tStart;
        this.cStart = cStart;
    }

    BucketMetadata(BucketMetadata that) {
        this(that.bucketID, that.tStart, that.cStart);
    }

    @Override
    public String toString() {
        return "<bucket " + bucketID +
                ", tStart " + tStart +
                ", cStart " + cStart +
                ">";
    }
}
