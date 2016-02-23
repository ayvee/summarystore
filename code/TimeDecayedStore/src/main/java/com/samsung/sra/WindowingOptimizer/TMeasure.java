package com.samsung.sra.WindowingOptimizer;

/**
 * Essentially a probability distribution over all time intervals contained in [0, N-1].
 * We call it a measure instead of a probability distribution because we don't insist on
 * normalization: \sum_[l, r] M[l, r] doesn't have to equal 1.
 *
 * Subclasses must override at least one of M_l_r and M_a_l.
 */
public abstract class TMeasure {
    public final int N;

    protected TMeasure(int N) {
        this.N = N;
    }

    /**
     * What is the measure of the interval [l, r]?
     */
    public double M_l_r(int l, int r) {
        return M_a_l(N - 1 - r, r - l + 1);
    }

    /**
     * What is the measure of the interval whose right endpoint has age a
     * and whose length is l?
     */
    public double M_a_l(int a, int l) {
        return M_l_r(N - l - a, N - 1 - a);
    }
}
