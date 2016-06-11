package com.samsung.sra.DataStore;

import java.io.Serializable;

/** All the code creating, manipulating, and querying buckets is in StreamManager */
public class Bucket implements Serializable {
    // data
    public Object[] aggregates;

    // metadata
    /* We use longs for bucket IDs, timestamps, and count markers. Valid values should be
       non-negative (all three are 0-indexed), and use "-1" to indicate null values. */
    // TODO: weaken access modifiers
    public long prevBucketID, thisBucketID, nextBucketID;
    public long tStart, tEnd;
    public long cStart, cEnd;

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
        ret += ", count range [" + cStart + ":" + cEnd + "]>";
        return ret;
    }
}
