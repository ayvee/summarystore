package com.samsung.sra.TimeDecayedStore;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.TreeMap;

class StreamInfo implements Serializable {
    public final StreamID streamID;
    // Object that readers and writers respectively will use "synchronized" with
    public final Object readerSyncObj = new Object(), writerSyncObj = new Object();
    // How many objects have we inserted so far?
    public int numElements = 0;

    // TODO: register an object to track what data structure we will use for each bucket

    /** All buckets for this stream */
    public final LinkedHashMap<BucketID, BucketInfo> buckets = new LinkedHashMap<BucketID, BucketInfo>();
    /** If there is an active (unclosed) landmark bucket, its ID */
    public BucketID activeLandmarkBucket = null;
    /** Index mapping bucket.startN -> bucketID, used to answer queries */
    public final TreeMap<Integer, BucketID> timeIndex = new TreeMap<Integer, BucketID>();

    StreamInfo(StreamID streamID) {
        this.streamID = streamID;
    }

    public void reconstructTimeIndex() {
        timeIndex.clear();
        for (BucketInfo bucketInfo: buckets.values()) {
            assert bucketInfo.startN >= 0;
            timeIndex.put(bucketInfo.startN, bucketInfo.bucketID);
        }
    }

}
