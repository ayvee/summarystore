package com.samsung.sra.WindowingOptimizer;

/**
 * Choose age and length according to independent identical Zipf distributions
 */
public class ZipfTMeasure extends TMeasure {
    //private final ZipfDistribution zdist;
    private final double s;

    public ZipfTMeasure(int N, double s) {
        super(N);
        this.s = s;
        //zdist = new ZipfDistribution(N, s);
    }

    @Override
    public double M_a_l(int a, int l) {
        assert 0 <= a && a <= N-1 && 1 <= l && l <= N;
        if (a + l > N) {
            return 0;
        } else {
            //return zdist.probability(a+1) * zdist.probability(l-1);
            return 1d / (Math.pow(a+1, s) * Math.pow(l, s));
        }
    }
}
