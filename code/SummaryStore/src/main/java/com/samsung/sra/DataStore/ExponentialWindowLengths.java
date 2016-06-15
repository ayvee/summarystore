package com.samsung.sra.DataStore;

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

    @Override
    public long getLength(int index) {
        double start = 1;

        for(int i = 0; i < index; i++){
            start *= base;
        }
        return (long)Math.ceil(start);
    }

    @Override
    public long getTotalLength(int numWindow) {
        double start = 1;
        double sum = start;

        for(int i = 0; i < numWindow - 1; i++){
            start *= base;
            sum += start;
        }
        return (long)Math.ceil(sum);
    }

}
