package com.samsung.sra.TimeDecayedStore;

import java.util.LinkedHashMap;

/**
 * Created by a.vulimiri on 1/20/16.
 */
class StreamInfo {
    final StreamID streamID;
    // Object that readers and writers respectively will use "synchronized" with
    final Object readerSyncObj, writerSyncObj;
    // How many objects have we inserted so far?
    int numElements;

    // TODO: register an object to track what data structure we will use for each bucket

    final LinkedHashMap<BucketID, BucketInfo> buckets; // should this be a Map or a List?

    StreamInfo(StreamID streamID) {
        this.streamID = streamID;
        this.readerSyncObj = new Object();
        this.writerSyncObj = new Object();
        this.buckets = new LinkedHashMap<BucketID, BucketInfo>();
        this.numElements = 0;
    }
}
