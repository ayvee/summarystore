package com.samsung.sra.TimeDecayedStore;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.*;

/**
 * Implements the time-decay and landmark parts of SummaryStore. API:
 *    register(streamID, aggregateDataStructure)
 *    append(streamID, List<FlaggedValue>)
 *    query(streamID, t1, t2, aggregateFunction)
 * At this point we make no distinction between time and count, i.e. we assume
 * exactly one element arrives at t = 0, 1, 2, 3, ...
 * Created by a.vulimiri on 1/15/16.
 */
public class TimeDecayedStore {
    private final String rocksDBPath;
    private BucketMerger merger;
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

        BucketInfo(BucketInfo that) {
            this(that.bucketID, that.startN, that.endN, that.isLandmark);
        }
    }

    /* The buckets proper are stored in RocksDB. We maintain additional in-memory indexes
    to help reads and writes:
        streamsInfo: basic stream metadata + list of bucket endpoints for each stream
        activeLandmarkBuckets: BucketIDs for streams that have an open landmark bucket
     Right now, the append operation is responsible for ensuring these indexes are synchronized
     with the base data on RocksDB.
     */
    private final Map<StreamID, StreamInfo> streamsInfo;
    private Map<StreamID, BucketID> activeLandmarkBuckets;

    public TimeDecayedStore(String rocksDBPath, BucketMerger merger) throws RocksDBException {
        /* TODO: implement a lock to ensure exclusive access to this RocksDB path.
                 RocksDB does not seem to have built-in locking */
        this.rocksDBPath = rocksDBPath;
        RocksDB.loadLibrary();
        rocksDBOptions = new Options().setCreateIfMissing(true);
        rocksDB = RocksDB.open(rocksDBOptions, rocksDBPath);
        this.merger = merger;

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

    public void append(final StreamID streamID, final List<FlaggedValue> values) throws StreamException, LandmarkEventException {
        final StreamInfo streamInfo;
        synchronized (streamsInfo) {
            if (!streamsInfo.containsKey(streamID)) {
                throw new StreamException("attempting to append to unregistered streamID " + streamID);
            } else {
                streamInfo = streamsInfo.get(streamID);
            }
        }

        synchronized (streamInfo.writerSyncObj) {
            /* All writes will be serialized at this point. We will also synchronize on the
             reader object below once we've done the bucket merge math and are ready to start
             modifying the data structure */
            int N0 = streamInfo.numElements, N = N0 + values.size();
            BucketID bucketID0, nextBucketID;
            if (streamInfo.numElements > 0) {
                bucketID0 = streamInfo.buckets.get(streamInfo.buckets.size() - 1).bucketID;
                nextBucketID = bucketID0.nextBucketID();
            } else {
                bucketID0 = null;
                nextBucketID = new BucketID(0);
            }

            Map<BucketID, LinkedHashMap<Integer, Object>> pendingInserts = new HashMap<BucketID, LinkedHashMap<Integer, Object>>();
            LinkedHashMap<BucketID, BucketInfo>
                    baseBuckets = new LinkedHashMap<BucketID, BucketInfo>(),
                    landmarkBuckets = new LinkedHashMap<BucketID, BucketInfo>();
            for (BucketInfo bucketInfo: streamInfo.buckets) {
                BucketInfo bucketInfoCopy = new BucketInfo(bucketInfo);
                if (bucketInfo.isLandmark) {
                    landmarkBuckets.put(bucketInfo.bucketID, bucketInfoCopy);
                } else {
                    baseBuckets.put(bucketInfo.bucketID, bucketInfoCopy);
                }
            }
            BucketID activeLandmarkBucket = activeLandmarkBuckets.get(streamID);
            for (int i = 0; i < values.size(); ++i) {
                // insert into appropriate bucket (creating the target bucket if necessary)
                int n = N0 + i;
                FlaggedValue fv = values.get(i);
                /* We always create a new base bucket of size 1 for every inserted element. As with any other
                 base bucket, it can be empty if the value at that position goes into a landmark bucket instead */
                BucketID newBaseBucket = nextBucketID;
                baseBuckets.put(newBaseBucket, new BucketInfo(newBaseBucket, n, n, false));
                nextBucketID = nextBucketID.nextBucketID();

                if (fv.landmarkStartsHere) {
                    // create a landmark bucket
                    if (activeLandmarkBucket != null) {
                        throw new LandmarkEventException();
                    }
                    activeLandmarkBucket = nextBucketID;
                    landmarkBuckets.put(activeLandmarkBucket, new BucketInfo(activeLandmarkBucket, n, n, true));
                    nextBucketID = nextBucketID.nextBucketID();
                }
                if (activeLandmarkBucket != null) {
                    // insert into active landmark bucket
                    assert landmarkBuckets.containsKey(activeLandmarkBucket);
                    landmarkBuckets.get(activeLandmarkBucket).endN = n;
                    if (!pendingInserts.containsKey(activeLandmarkBucket)) {
                        pendingInserts.put(activeLandmarkBucket, new LinkedHashMap<Integer, Object>());
                    }
                    pendingInserts.get(activeLandmarkBucket).put(n, fv.value);
                } else {
                    // insert into the new base bucket of size 1
                    if (!pendingInserts.containsKey(newBaseBucket)) {
                        pendingInserts.put(newBaseBucket, new LinkedHashMap<Integer, Object>());
                    }
                    pendingInserts.get(newBaseBucket).put(n, fv.value);
                }
                if (fv.landmarkEndsHere) {
                    /* Close the landmark bucket. Right now we don't track open/closed in the bucket explicitly,
                    so all we need to do is unmark activeLandmarkBucket */
                    if (activeLandmarkBucket == null) {
                        throw new LandmarkEventException();
                    }
                    activeLandmarkBucket = null;
                }
            }

            List<List<BucketID>> pendingMerges = merger.merge(baseBuckets, N0, N);
        }
    }

    public void close() {
        // FIXME: should wait for any in-process appends to terminate first
        if (rocksDB != null) {
            rocksDB.close();
        }
        rocksDBOptions.dispose();
    }

    public static void main(String[] args) {
        TimeDecayedStore store = null;
        try {
            store = new TimeDecayedStore("/tmp/tdstore", new WBMHBucketMerger());
        } catch (RocksDBException e) {
            e.printStackTrace();
        } finally {
            if (store != null) {
                store.close();
            }
        }
    }
}