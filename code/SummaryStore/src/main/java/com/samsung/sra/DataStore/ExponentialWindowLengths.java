package com.samsung.sra.DataStore;

class ExponentialWindowLengths implements WindowLengthsGenerator{
    private long next = 1;
    private final long base;

    ExponentialWindowLengths(int base) {
        this.base = base;
    }

    @Override
    public long nextWindowLength() {
        long prev = next;
        next *= base;
        return prev;
    }

    @Override
    public long howManyWindowsToCover(long rangeSize) {
        return (long)Math.ceil(Math.log(rangeSize) / Math.log(base));
    }
}
