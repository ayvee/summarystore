package com.samsung.sra.DataStore;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.TreeMap;

class StreamInfo implements Serializable {
    final StreamID streamID;
    // Object that readers and writers respectively will use "synchronized" with (acts as a mutex)
    final Object readerSyncObj = new Object(), writerSyncObj = new Object();
    // How many values have we inserted so far?
    int numValues = 0;
    // What was the timestamp of the latest value appended?
    Timestamp lastValueTimestamp = null;
    // FIXME: Implicit assumption here that time starts at 0 in every stream; we can discuss if that should change

    // TODO: register an object to track what data structure we will use for each bucket

    /** All buckets for this stream */
    final LinkedHashMap<BucketID, BucketMetadata> buckets = new LinkedHashMap<BucketID, BucketMetadata>();
    /** If there is an active (unclosed) landmark bucket, its ID */
    BucketID activeLandmarkBucket = null;
    /** Index mapping bucket.tStart -> bucketID, used to answer queries */
    final TreeMap<Timestamp, BucketID> timeIndex = new TreeMap<Timestamp, BucketID>();

    StreamInfo(StreamID streamID) {
        this.streamID = streamID;
    }

    void reconstructTimeIndex() {
        timeIndex.clear();
        for (BucketMetadata bucketMetadata : buckets.values()) {
            assert bucketMetadata.tStart != null;
            timeIndex.put(bucketMetadata.tStart, bucketMetadata.bucketID);
        }
    }
}
