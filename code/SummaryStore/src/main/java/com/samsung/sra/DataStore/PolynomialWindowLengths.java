package com.samsung.sra.DataStore;

/**
 * scale * 1^degree, scale * 2^degree, scale * 3^degree, ...
 */
public class PolynomialWindowLengths extends WindowLengths {
    private final double scale;
    private final int degree;
    private long n = 0;

    public PolynomialWindowLengths(double scale, int degree) {
        this.scale = scale;
        this.degree = degree;
    }

    public long nextWindowLength() {
        return (long)Math.ceil(scale * Math.pow(++n, degree));
    }

    @Override
    public long getWindowLengthUpperBound(long N) {
        if (degree == 0) {
            return (long)Math.ceil(scale);
        } else {
            return Long.MAX_VALUE;
        }
    }

    /** Returns a polynomial sequence of specified degree scaled so that numWindows windows will
     * be needed to cover [0, rangeSize)
     */
    public static WindowLengths getWindowingOfSize(int degree, long rangeSize, long numWindows) {
        assert rangeSize > 0 && numWindows > 0 && numWindows <= rangeSize;
        double unscaled = 0;
        for (long n = 1; n <= numWindows; ++n) {
            unscaled += Math.pow(n, degree);
        }
        double scale = rangeSize / unscaled; // FIXME: does not account for rounding (Math.ceil above)
        return new PolynomialWindowLengths(scale, degree);
    }
}
