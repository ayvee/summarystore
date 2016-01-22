package com.samsung.sra.TimeDecayedStore;

/**
 * Values with associated flags marking out landmark start and end events.
 * Created by a.vulimiri on 1/19/16.
 */
public class FlaggedValue {
    public Object value;
    public boolean landmarkStartsHere, landmarkEndsHere;

    public FlaggedValue(Object value, boolean landmarkStartsHere, boolean landmarkEndsHere) {
        this.value = value;
        this.landmarkStartsHere = landmarkStartsHere;
        this.landmarkEndsHere = landmarkEndsHere;
    }

    public FlaggedValue(Object value) {
        this(value, false, false);
    }
}
