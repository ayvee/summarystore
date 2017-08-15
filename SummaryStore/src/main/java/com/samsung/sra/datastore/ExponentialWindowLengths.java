package com.samsung.sra.datastore;

/**
 * 1, b, b^2, ..., b^k, ...
 */
public class ExponentialWindowLengths implements WindowLengthsSequence {
    private double next = 1;
    private final double base;

    public ExponentialWindowLengths(double base) {
        this.base = base;
    }

    @Override
    public long nextWindowLength() {
        double prev = next;
        next *= base;
        return (long)Math.ceil(prev);
    }
}
