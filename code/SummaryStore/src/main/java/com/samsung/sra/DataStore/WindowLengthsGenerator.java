package com.samsung.sra.DataStore;

/**
 * Generate an infinite sequence of window lengths.
 * E.g. 1, 2, 4, 8, 16, ...
 */
interface WindowLengthsGenerator {
    long nextWindowLength();

    /**
     * How many windows would be needed to span [1, rangeSize]?
     */
    long howManyWindowsToCover(long rangeSize);
}
