package com.samsung.sra.TimeDecayedStore;

/**
 * Values with associated tags marking out landmark start and end events.
 * Created by a.vulimiri on 1/19/16.
 */
public class TaggedValue {
    public static enum Event {
        LANDMARK_START,
        LANDMARK_END
    }

    public final Object value;
    public final Event associatedEvent;

    public TaggedValue(Object value, Event associatedEvent) {
        this.value = value;
        this.associatedEvent = associatedEvent;
    }

    public TaggedValue(Object value) {
        this(value, null);
    }
}
