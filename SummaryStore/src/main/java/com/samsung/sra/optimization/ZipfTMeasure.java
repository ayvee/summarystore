package com.samsung.sra.optimization;

/**
 * Choose age and length according to independent identical Zipf distributions
 */
public class ZipfTMeasure extends TMeasure {
    private final double s;
    private double normfact = 1;

    public ZipfTMeasure(int N, double s) {
        super(N);
        this.s = s;
        normfact = computeNormFact();
    }

    private double computeNormFact() {
        double sum = 0;
        for (int l = 0; l < N; ++l) {
            for (int r = l; r < N; ++r) {
                sum += M_l_r(l, r);
            }
        }
        return 1d / sum;
    }

    @Override
    public double M_a_l(int a, int l) {
        assert 0 <= a && a <= N-1 && 1 <= l && l <= N;
        if (a + l > N) {
            return 0;
        } else {
            return normfact / (Math.pow(a+1, s) * Math.pow(l, s));
        }
    }
}
