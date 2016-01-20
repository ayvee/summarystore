package com.samsung.sra.TimeDecayedStore;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.*;
import java.util.function.ObjDoubleConsumer;

/**
 * Implements the time-decay and landmark parts of SummaryStore. API:
 *    register(streamID, aggregateDataStructure)
 *    append(streamID, List<Value>)
 *    query(aggregateFunction, t1, t2)
 * At this point we make no distinction between time and count, i.e. we assume
 * exactly one element arrives at t = 0, 1, 2, 3, ...
 * Created by a.vulimiri on 1/15/16.
 */
public class TimeDecayedStore {
    private final String rocksDBPath;
    private RocksDB rocksDB = null;
    private Options rocksDBOptions = null;

    static class StreamInfo {
        final StreamID streamID;
        // Object that readers and writers respectively will use "synchronized" with
        final Object readerSyncObj, writerSyncObj;
        // How many objects have we inserted so far?
        int numElements;

        // TODO: register an object to track what data structure we will use for each bucket

        final List<BucketInfo> buckets; // should this be a Map or a List?

        StreamInfo(StreamID streamID) {
            this.streamID = streamID;
            this.readerSyncObj = new Object();
            this.writerSyncObj = new Object();
            this.buckets = new ArrayList<BucketInfo>();
            this.numElements = 0;
        }
    }

    static class BucketInfo {
        final BucketID bucketID;
        int startN, endN;
        boolean isLandmark;

        BucketInfo(BucketID bucketID, int startN, int endN, boolean isLandmark) {
            this.bucketID = bucketID;
            this.startN = startN;
            this.endN = endN;
            this.isLandmark = isLandmark;
        }
    }

    private final Map<StreamID, StreamInfo> streamsInfo;
    private Map<StreamID, BucketID> activeLandmarkBuckets;

    public TimeDecayedStore(String rocksDBPath) throws RocksDBException {
        /* TODO: implement a lock to ensure exclusive access to this RocksDB path.
                 RocksDB does not seem to have built-in locking */
        this.rocksDBPath = rocksDBPath;
        RocksDB.loadLibrary();
        rocksDBOptions = new Options().setCreateIfMissing(true);
        rocksDB = RocksDB.open(rocksDBOptions, rocksDBPath);

        this.streamsInfo = new HashMap<StreamID, StreamInfo>();
        this.activeLandmarkBuckets = new HashMap<StreamID, BucketID>();
    }

    public void registerStream(final StreamID streamID) throws StreamException {
        // TODO: also register what data structure we will use for each bucket
        synchronized (streamsInfo) {
            if (streamsInfo.containsKey(streamID)) {
                throw new StreamException("attempting to register streamID " + streamID + " multiple times");
            } else {
                streamsInfo.put(streamID, new StreamInfo(streamID));
                activeLandmarkBuckets.put(streamID, null);
            }
        }
    }

    public void append(final StreamID streamID, final List<Value> values) throws StreamException, LandmarkEventException {
        final StreamInfo streamInfo;
        synchronized (streamsInfo) {
            if (!streamsInfo.containsKey(streamID)) {
                throw new StreamException("attempting to append to unregistered streamID " + streamID);
            } else {
                streamInfo = streamsInfo.get(streamID);
            }
        }

        class PendingBucketActions {
            final BucketID bucketID;
            List<BucketID> bucketsToMerge;
            Map<Integer, Object> valuesToInsert;

            int startN, endN;
            boolean isLandmark;

            PendingBucketActions(BucketID bucketID, int startN, int endN, boolean isLandmark) {
                this.bucketID = bucketID;
                bucketsToMerge = new ArrayList<BucketID>();
                valuesToInsert = new LinkedHashMap<Integer, Object>();

                this.startN = startN;
                this.endN = endN;
                this.isLandmark = isLandmark;
            }

            PendingBucketActions(BucketInfo bucketInfo) {
                this(bucketInfo.bucketID, bucketInfo.startN, bucketInfo.endN, bucketInfo.isLandmark);
            }
        }

        synchronized (streamInfo.writerSyncObj) {
            /* All writes will be serialized at this point. We will also synchronize on the
             reader object below once we've done the math and are ready to start modifying
             the data structure */
            int N0 = streamInfo.numElements, N = N0 + values.size();
            BucketID bucketID0, nextBucketID;
            if (streamInfo.numElements > 0) {
                bucketID0 = streamInfo.buckets.get(streamInfo.buckets.size() - 1).bucketID;
                nextBucketID = bucketID0.nextBucketID();
            } else {
                bucketID0 = null;
                nextBucketID = new BucketID(0);
            }

            BucketID activeLandmarkBucket = activeLandmarkBuckets.get(streamID);
            Map<BucketID, PendingBucketActions> pendingBucketActions = new LinkedHashMap<BucketID, PendingBucketActions>();
            for (BucketInfo bucketInfo: streamInfo.buckets) {
                pendingBucketActions.put(bucketInfo.bucketID, new PendingBucketActions(bucketInfo));
            }
            for (int i = 0; i < values.size(); ++i) {
                int n = N0 + i;
                Value v = values.get(i);
                /* We always create a new base bucket of size 1 for every inserted element. As with any other
                 base bucket, it can be empty if the value at that position goes into a landmark bucket instead */
                PendingBucketActions newBaseBucketActions = new PendingBucketActions(nextBucketID, n, n, false);
                nextBucketID = nextBucketID.nextBucketID();
                pendingBucketActions.put(newBaseBucketActions.bucketID, newBaseBucketActions);

                if (v.event == Value.Event.LANDMARK_START) {
                    // create a landmark bucket
                    if (activeLandmarkBucket != null) {
                        throw new LandmarkEventException();
                    }
                    activeLandmarkBucket = nextBucketID;
                    nextBucketID = nextBucketID.nextBucketID();
                    PendingBucketActions landmarkActions = new PendingBucketActions(activeLandmarkBucket, n, n, true);
                    pendingBucketActions.put(landmarkActions.bucketID, landmarkActions);
                }
                if (activeLandmarkBucket != null) {
                    // insert element into landmark bucket
                    PendingBucketActions landmarkActions = pendingBucketActions.get(activeLandmarkBucket);
                    landmarkActions.valuesToInsert.put(n, v.value);
                    landmarkActions.endN = n;
                } else {
                    // insert into the new base bucket of size 1
                    newBaseBucketActions.valuesToInsert.put(n, v.value);
                }
                if (v.event == Value.Event.LANDMARK_END) {
                    /* Close the landmark bucket. Right now we don't track open/closed in the bucket explicitly,
                    so all we need to do is unmark activeLandmarkBucket */
                    if (activeLandmarkBucket == null) {
                        throw new LandmarkEventException();
                    }
                    activeLandmarkBucket = null;
                }
            }
        }
    }

    public void close() {
        if (rocksDB != null) {
            rocksDB.close();
        }
        rocksDBOptions.dispose();
    }

    public static void main(String[] args) {
        TimeDecayedStore store = null;
        try {
            store = new TimeDecayedStore("/tmp/tdstore");
        } catch (RocksDBException e) {
            e.printStackTrace();
        } finally {
            if (store != null) {
                store.close();
            }
        }
    }
}