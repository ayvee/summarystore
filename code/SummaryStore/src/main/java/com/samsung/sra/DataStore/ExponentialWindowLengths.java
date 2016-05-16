package com.samsung.sra.DataStore;

import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.commons.math3.analysis.solvers.BrentSolver;

/**
 * 1, b, b^2, ..., b^k, ...
 */
public class ExponentialWindowLengths extends WindowLengths {
    private double next = 1;
    private final double base;

    public ExponentialWindowLengths(double base) {
        this.base = base;
    }

    @Override
    public long nextWindowLength() {
        double prev = next;
        next *= base;
        return (long)Math.ceil(prev);
    }

    @Override
    public long getWindowLengthUpperBound(long N) {
        if (N == -1) {
            return Long.MAX_VALUE;
        } else {
            // return largest power of base smaller than N
            return (long)Math.ceil(Math.pow(base, Math.floor(Math.log(N) / Math.log(base))));
        }
    }

    /**
     * Return an exponential windowing that will use numWindows windows to cover [0, rangeSize)
     */
    public static ExponentialWindowLengths getWindowingOfSize(final long rangeSize, long numWindows) {
        assert numWindows > 0 && rangeSize > 0 && numWindows <= rangeSize;
        double base;
        if (numWindows == 1) {
            base = rangeSize;
        } else if (numWindows == rangeSize) {
            base = 1;
        } else {
            // f(b) = (# of windows that exp(b) would use to cover rangeSize) - numWindows
            UnivariateFunction f = (double b) -> {
                long W = 0;
                long N = rangeSize;
                double size = 1;
                while (N > 0) {
                    N -= (long)Math.ceil(size);
                    ++W;
                    size *= b;
                }
                return W - numWindows;
            };
            // solve for f(b) == 0
            base = (new BrentSolver()).solve((int)1e9, f, 1 + 1e-10, rangeSize);
        }
        return new ExponentialWindowLengths(base);
    }
}
