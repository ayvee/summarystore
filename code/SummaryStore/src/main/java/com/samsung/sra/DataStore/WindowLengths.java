package com.samsung.sra.DataStore;

import java.io.Serializable;
import java.util.function.LongConsumer;

/**
 * Generate an infinite sequence of window lengths.
 * E.g. 1, 2, 4, 8, 16, ...
 */
public abstract class WindowLengths implements Serializable {
    public abstract long nextWindowLength();

    /**
     * What is the largest window size in a windowing covering N elements?
     * null N represents infinity. Should return Long.MAX_VALUE if
     * the answer is unbounded
     */
    public abstract long getWindowLengthUpperBound(long N);

    /**
     * Keep generating window lengths, passing them to the consumer, until we hit a window
     * of specified target length. Returns false if the target length is too large to achieve.
     * If the optional argument N is specified, it will be passed onto getWindowLengthUpperBound
     * when computing the largest achievable window length.
     */
    boolean addWindowsUntilLength(long targetLength, LongConsumer consumer, long N) {
        if (targetLength > getWindowLengthUpperBound(N)) {
            return false;
        } else {
            long genLength;
            do {
                genLength = nextWindowLength();
                consumer.accept(genLength);
            } while (genLength < targetLength);
            return true;
        }
    }

    boolean addWindowsUntilLength(long targetLength, LongConsumer consumer) {
        return addWindowsUntilLength(targetLength, consumer, -1);
    }
}
