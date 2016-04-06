package com.samsung.sra.DataStore;

/**
 * Generate an infinite sequence of window lengths.
 * E.g. 1, 2, 4, 8, 16, ...
 */
public interface WindowLengthSequence {
    long nextWindowLength();
}
