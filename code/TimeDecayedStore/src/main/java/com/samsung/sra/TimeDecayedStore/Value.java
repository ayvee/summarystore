package com.samsung.sra.TimeDecayedStore;

/**
 * Created by a.vulimiri on 1/19/16.
 */
public class Value {
    public static enum Event {
        LANDMARK_START, // set on
        LANDMARK_END,
        NONE
    }

    public final Event event;
    public final Object value;

    public Value(Event event, Object value) {
        this.event = event;
        this.value = value;
    }
}
