package com.samsung.sra.optimization;

/**
 * Linear error: error = amount of overhang. Happens to be the
 * error function for the count upper bound data structure
 */
public class LinearEFunc implements EFunc {
    public double e(int l, int q) {
        assert l >= q;
        return l - q;
    }
}
