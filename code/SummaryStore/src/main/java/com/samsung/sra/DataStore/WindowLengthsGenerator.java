package com.samsung.sra.DataStore;

/**
 * Generate an infinite sequence of window lengths.
 * E.g. 1, 2, 4, 8, 16, ...
 */
interface WindowLengthsGenerator {
    int nextWindowLength();

    /**
     * How many windows would be needed to span [1, rangeSize]?
     */
    int howManyWindowsToCover(long rangeSize);
}
