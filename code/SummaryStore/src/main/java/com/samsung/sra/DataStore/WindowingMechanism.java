package com.samsung.sra.DataStore;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * Encapsulates code for EH/WBMH/similar mechanisms
 */
interface WindowingMechanism {
    /**
     * Figure out what bucket modifications need to be performed after a new element is inserted.
     * Each modification will either merge a consecutive sequence of existing base buckets or
     * create a new base bucket at the end of the list (after all the existing buckets).
     * If creating new buckets, use BucketID.nextBucketID() to assign IDs to them: this will
     * maintain the invariant that recent buckets always have larger BucketIDs than older buckets.
     *
     * The existingBuckets argument is the list of all existing buckets (base as well as landmark)
     * sorted by age oldest first. It should not be modified. (FIXME? Maybe pass in a copy instead)
     *
     * If newValue is not a landmark value, it will be inserted into the last base bucket in the list after
     * all the update operations have been processed. The new (timestamp, value) are always provided to
     * this class, even if this is a landmark value, because some windowing mechanisms can use the pair
     * to guide their choice of windowing (e.g. to update a burst-detection counter).
     */
    List<SummaryStore.BucketModification> computeModifications(
            LinkedHashMap<BucketID, BucketMetadata> existingBuckets,
            int numValuesSoFar, Timestamp lastInsertedTimestamp,
            Timestamp newValueTimestamp, Object newValue, boolean isLandmarkValue);
}
