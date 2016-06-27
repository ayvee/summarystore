package com.samsung.sra.DataStore;

import java.io.Serializable;

/** Dumb structs. All the code creating, manipulating, and querying buckets is in StreamManager */
public class Bucket implements Serializable {
    // metadata
    /* We use longs for bucket IDs, timestamps, and count markers. Valid values should be
       non-negative (all three are 0-indexed); use "-1" to indicate null values. */
    // TODO: weaken access modifiers
    public long prevBucketID, thisBucketID, nextBucketID;
    public long tStart, tEnd;
    public long cStart, cEnd;

    // data
    public Object[] aggregates;

    public static final int METADATA_BYTECOUNT = 7 * 8;

    Bucket() {}

    Bucket(WindowOperator[] operators,
           long prevBucketID, long thisBucketID, long nextBucketID,
           long tStart, long tEnd, long cStart, long cEnd) {
        this.prevBucketID = prevBucketID;
        this.thisBucketID = thisBucketID;
        this.nextBucketID = nextBucketID;
        this.tStart = tStart;
        this.tEnd = tEnd;
        this.cStart = cStart;
        this.cEnd = cEnd;
        aggregates = new Object[operators.length];
        for (int i = 0; i < aggregates.length; ++i) {
            aggregates[i] = operators[i].createEmpty(); // empty aggr
        }
    }

    @Override
    public String toString() {
        String ret = "<bucket " + thisBucketID;
        ret += ", time range [" + tStart + ":" + tEnd + "]";
        ret += ", count range [" + cStart + ":" + cEnd + "]";
        ret += ", count = " + (cEnd - cStart + 1) + ">";
        return ret;
    }
}
