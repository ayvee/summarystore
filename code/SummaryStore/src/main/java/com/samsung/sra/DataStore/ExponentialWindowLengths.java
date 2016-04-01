package com.samsung.sra.DataStore;

class ExponentialWindowLengths implements WindowLengthsGenerator{
    private int next = 1;
    private final int base;

    ExponentialWindowLengths(int base) {
        this.base = base;
    }

    @Override
    public int nextWindowLength() {
        int prev = next;
        next *= base;
        return prev;
    }

    @Override
    public int howManyWindowsToCover(long rangeSize) {
        return (int)Math.ceil(Math.log(rangeSize) / Math.log(base));
    }
}
