package com.samsung.sra.DataStore;

/**
 * scale * 1^degree, scale * 2^degree, scale * 3^degree, ...
 */
public class PolynomialWindowLengths extends WindowLengths {
    private final double scale, degree;
    private int n = 0;

    public PolynomialWindowLengths(double scale, double degree) {
        this.scale = scale;
        this.degree = degree;
    }

    public long nextWindowLength() {
        return (long)Math.ceil(scale * Math.pow(++n, degree));
    }

    @Override
    public long getWindowLengthUpperBound(Long N) {
        return 0;
    }

    /** Returns a polynomial sequence of specified degree scaled so that numWindows windows will
     * be needed to cover [0, rangeSize)
     */
    public static WindowLengths getWindowingOfSize(double degree, long rangeSize, int numWindows) {
        assert rangeSize > 0 && numWindows > 0 && numWindows <= rangeSize;
        double unscaled = 0;
        for (int n = 1; n <= numWindows; ++n) {
            unscaled += Math.pow(n, degree);
        }
        double scale = rangeSize / unscaled;
        return new PolynomialWindowLengths(scale, degree);
    }
}
