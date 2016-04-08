package com.samsung.sra.DataStore;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;

/**
 * Encapsulates code for EH/WBMH/similar mechanisms
 */
public interface WindowingMechanism extends Serializable {
    /**
     * Figure out what bucket modifications need to be performed after a new element is inserted.
     * Each modification will either merge a consecutive sequence of existing base buckets or
     * create a new base bucket after all the existing buckets. If creating new buckets, use
     * nextBucketID() to assign IDs to them: this will maintain the invariant that recent buckets
     * always have larger BucketIDs than older buckets.
     *
     * The implementor is responsible for maintaining any internal state necessary to figure out
     * what merges are needed, e.g. start and end timestamps for each existing bucket.
     * (Code is written this way because different windowing mechanisms can want very different
     * data structures to maintain this state, e.g. compare CountBasedWBMH and SlowCountBasedWBMH)
     *
     * SummaryStore will first process all modifications returned by this function, then insert
     * (newTimestamp, newValue) into the appropriate bucket
     */
    List<SummaryStore.BucketModification> computeModifications(Timestamp newValueTimestamp, Object newValue);
}
