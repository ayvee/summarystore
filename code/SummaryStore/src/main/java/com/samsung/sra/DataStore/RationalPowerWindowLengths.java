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
        if (N == -1) {
            return Long.MAX_VALUE;
        }
        if (q == 0) {
            return 1;
        } else {
            // calculate upper bound assuming fastest possible growth, i.e. 1, 2, 3, 4, ..., U
            // U(U+1)/2 = N => U = sqrt(U^2) <= sqrt(2N)
            return (long)Math.ceil(Math.sqrt(2 * N));
        }
    }
}
