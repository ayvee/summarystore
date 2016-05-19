package com.samsung.sra.DataStore;

import java.io.Serializable;

/**
 * These are the two properties of a window sequence that WBMH actually needs to know.
 * The GenericWindowing implementor can build a Windowing out of any arbitrary
 * WindowLengthsSequence, but it can be very time/space inefficient for e.g. gradual
 * decay functions, and specialized implementations can potentially be much faster
 */
public interface Windowing extends Serializable {
    /**
     * Return the first T' >= T such that at time T' the interval [l, r] is contained
     * inside a single window. l and r are absolute timestamps (not ages)
     */
    long getFirstContainingTime(long l, long r, long T);

    long getSizeOfFirstWindow();
}
