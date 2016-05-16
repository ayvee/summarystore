package com.samsung.sra.DataStore;

/**
 * 1, 2^(p-1) of size 2^q, ..., k^(p-1) of size k^q, ...
 * Decay b(n) = O(n^(-q/(p+q)))
 */
public class RationalPowerWindowLengths extends WindowLengths {
    private final long p, q;
    private long k = 1, currLength = 1, numLeftAtCurrLength = 1;

    public RationalPowerWindowLengths(long p, long q) {
        if (p < 1 || q < 0) throw new IllegalArgumentException("invalid p or q");
        this.p = p;
        this.q = q;
    }
    @Override
    public long nextWindowLength() {
        if (numLeftAtCurrLength == 0) {
            ++k;
            currLength = (long) Math.pow(k, q);
            numLeftAtCurrLength = (long) Math.pow(k, p - 1);
        }
        --numLeftAtCurrLength;
        assert numLeftAtCurrLength >= 0;
        return currLength;
    }

    @Override
    public long getWindowLengthUpperBound(long N) {
        return q == 0 ? 1 : Long.MAX_VALUE;
    }
}
