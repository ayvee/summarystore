package com.samsung.sra.DataStore;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;

/**
 * Encapsulates code for EH/WBMH/similar mechanisms
 */
public interface WindowingMechanism {
    /**
     * Figure out what bucket modifications need to be performed after a new element is inserted.
     * Each modification will either merge a consecutive sequence of existing base buckets or
     * create a new base bucket at the end of the list (after all the existing buckets).
     * If creating new buckets, use (last bucketID in list).nextBucketID() to assign IDs to them:
     * this will maintain the invariant that recent buckets always have larger BucketIDs than older
     * buckets.
     *
     * The existingBuckets argument is the list of all existing buckets sorted by age oldest first.
     * It should not be modified. (FIXME? Maybe pass in a copy instead)
     *
     * (newTimestamp, newValue) will be inserted into the last base bucket in the list after
     * all the update operations have been processed
     */
    List<SummaryStore.BucketModification> computeModifications(
            TreeMap<BucketID, BucketMetadata> existingBuckets,
            long numValuesSoFar, Timestamp lastInsertedTimestamp,
            Timestamp newValueTimestamp, Object newValue);
}
