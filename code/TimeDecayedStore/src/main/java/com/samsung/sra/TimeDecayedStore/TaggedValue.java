package com.samsung.sra.TimeDecayedStore;

/**
 * Values with associated tags marking out landmark start and end events.
 * Created by a.vulimiri on 1/19/16.
 */
public class TaggedValue {
    public final Object value;
    public final boolean landmarkStartsHere, landmarkEndsHere;

    public TaggedValue(Object value, boolean landmarkStartsHere, boolean landmarkEndsHere) {
        this.value = value;
        this.landmarkStartsHere = landmarkStartsHere;
        this.landmarkEndsHere = landmarkEndsHere;
    }

    public TaggedValue(Object value) {
        this(value, false, false);
    }
}
