package com.samsung.sra.optimization;

/**
 * Uniform random distribution over all time intervals
 */
public class UniformTMeasure extends TMeasure {
    public UniformTMeasure(int N) {
        super(N);
    }

    @Override
    public double M_l_r(int l, int r) {
        return 1;
    }
}
