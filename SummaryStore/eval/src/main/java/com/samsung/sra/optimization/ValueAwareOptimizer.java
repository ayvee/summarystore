/*
* Copyright 2016 Samsung Research America. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.samsung.sra.optimization;

import java.util.ArrayList;
import java.util.List;

/**
 * Variant of the UpperBoundOptimizer (which implements
 * https://confluence.sisa.samsung.com:8443/display/summarystore/Time-decayed+aggregation)
 * that optimizes based on the actual observed error for the given time series,
 * as opposed to the worst-case upper bound on the error estimate that UpperBoundOptimizer
 * uses. Running time is O(N^4) vs UpperBoundOptimizer's O(N^3)
 */
public class ValueAwareOptimizer {
    private final int N;
    private final TMeasure P;
    private final long[] counts;

    private double[][] Ml, Mr, Mvals;
    private long[][] trueCounts;

    public ValueAwareOptimizer(int N, TMeasure P, long[] counts) {
        assert counts.length == N;
        this.N = N;
        this.P = P;
        this.counts = counts;

        Ml = new double[N][N]; Mr = new double[N][N]; Mvals = new double[N][N];
        trueCounts = new long[N][N];
        compute_Ml(Ml);
        compute_Mr(Mr);
        compute_Mvals(Mvals);
        compute_trueCounts(trueCounts);
    }

    private void compute_Ml(double[][] Ml) {
        for (int l = 0; l < N; ++l) {
            for (int r = l; r < N; ++r) {
                if (l == 0) {
                    Ml[l][r] = 0;
                } else {
                    Ml[l][r] = Ml[l-1][r] + P.M_l_r(l-1, r);
                }
            }
        }
    }

    private void compute_Mr(double[][] Mr) {
        for (int l = 0; l < N; ++l) {
            for (int r = N-1; r >= l; --r) {
                if (r == N-1) {
                    Mr[l][r] = 0;
                } else {
                    Mr[l][r] = Mr[l][r+1] + P.M_l_r(l, r+1);
                }
            }
        }
    }

    private void compute_trueCounts(long[][] trueCounts) {
        for (int l = 0; l < N; ++l) {
            //if (l % 10 == 0) System.err.println("[" + LocalDateTime.now() + "] Computing trueCounts for l = " + l);
            for (int r = 0; r < N; ++r) {
                if (r < l) {
                    trueCounts[l][r] = 0;
                } else {
                    trueCounts[l][r] = (r > 0 ? trueCounts[l][r-1] : 0) + counts[r];
                }
            }
        }
        /*// test
        int l = 25, r = 73;
        int tc = 0;
        for (int t = l; t <= r; ++t) {
            tc += counts[t];
        }
        if (tc != trueCounts[l][r]) throw new IllegalStateException();*/
    }

    private void compute_Mvals(double[][] Mvals) {
        for (int l = 0; l < N; ++l) {
            for (int r = 0; r < N; ++r) {
                Mvals[l][r] = r < l ? 0 : P.M_l_r(l, r);
            }
        }
    }

    private static double getError(long trueval, long estimate) {
        /*// https://en.wikipedia.org/wiki/Symmetric_mean_absolute_percentage_error
        //long nr = estimate - trueval, dr = estimate + trueval;
        //return dr > 0 ? nr / (double)dr : 0;*/
        // above doesn't work: SMAPE isn't additive over intervals. Optimizing
        // absolute error instead
        assert estimate >= trueval;
        return estimate - trueval;
    }

    private void compute_E(double[][] E) {
        for (int l = 0; l < N; ++l) {
            //System.err.println("[" + LocalDateTime.now() + "] Computing E for l = " + l);
            //System.err.flush();
            for (int r = l; r < N; ++r) {
                //System.err.println("[" + LocalDateTime.now() + "] Computing E for [" + l + ", " + r + "]");
                E[l][r] = 0;
                for (int j = l; j <= r; ++j) {
                    E[l][r] += Ml[l][j] * getError(trueCounts[l][j], trueCounts[l][r]);
                }
                for (int i = l; i <= r; ++i) {
                    E[l][r] += Mr[i][r] * getError(trueCounts[i][r], trueCounts[l][r]);
                }
                for (int i = l; i <= r; ++i) {
                    for (int j = i; j <= r; ++j) {
                        E[l][r] += Mvals[i][j] * getError(trueCounts[i][j], trueCounts[l][r]);
                    }
                }
            }
        }
    }

    private double[][] E = null;

    private double[][] compute_E() {
        if (E == null) {
            E = new double[N][N];
            compute_E(E);
        }
        return E;
    }

    void print_E() {
        double[][] E = compute_E();
        for (int i = 0; i < N; ++i) {
            for (int j = 0; j < N; ++j) {
                System.err.print("\t" + E[i][j]);
            }
            System.err.println();
        }
    }

    public List<Integer> optimize(int W) {
        double[][] E = compute_E();
        double[][] C = new double[N+1][W+1];
        // right_endpoints[i][B] = j such that [i, j] is the first interval in an optimal B-byte windowing of [i, N-1]
        int[][] right_endpoints = new int[N+1][W+1];
        for (int B = 0; B <= W; ++B) {
            for (int i = N; i >= 0; --i) {
                if (i == N) {
                    C[i][B] = 0;
                } else if (B == 0) { // && i < N
                    C[i][B] = Double.POSITIVE_INFINITY;
                } else {
                    C[i][B] = Double.MAX_VALUE;
                    for (int j = i; j <= N-1; ++j) {
                        double newC = E[i][j] + C[j+1][B-1];
                        if (newC < C[i][B]) {
                            C[i][B] = newC;
                            right_endpoints[i][B] = j;
                        }
                    }
                }
            }
        }
        List<Integer> lengths = new ArrayList<Integer>();
        int i = 0;
        for (int B = W; B >= 1; --B) {
            int j = right_endpoints[i][B];
            lengths.add(j - i + 1);
            i = j + 1;
        }
        //assert Math.abs(C[0][W] - getCost(lengths)) < 1e-5;
        return lengths;
    }

    public long getQueryEstimateLR(List<Integer> windowLengths, int l, int r) {
        assert 0 <= l && l <= r && r < N;
        long ec = 0;
        int i = 0;
        for (Integer length: windowLengths) {
            int j = i + length - 1;
            if (l <= j && r >= i) {
                ec += trueCounts[i][j];
            }
            i = j + 1;
        }
        assert i == N;
        return ec;
    }

    public long getQueryTruevalLR(int l, int r) {
        return trueCounts[l][r];
    }

    public double getQueryRelativeErrorLR(List<Integer> windowLengths, int l, int r) {
        long estimate = getQueryEstimateLR(windowLengths, l, r);
        long trueval = getQueryTruevalLR(l, r);
        return trueval > 0 ? (estimate - trueval) / (double)(trueval) : 0;
    }

    public double getQueryRelativeErrorAL(List<Integer> windowLengths, int a, int l) {
        return getQueryRelativeErrorLR(windowLengths, N - l - a, N - 1 - a);
    }

    public double getQueryMAPELR(List<Integer> windowLengths, int l, int r) {
        long estimate = getQueryEstimateLR(windowLengths, l, r);
        long trueval = getQueryTruevalLR(l, r);
        return estimate > 0 ? (estimate - trueval) / (double)(estimate + trueval) : 0;
    }

    public double getWAPE(List<Integer> windowLengths) {
        /* Return weighted absolute % error: weighted_sum(est - true) / weighted_sum(true).
        Why WAPE:
            http://www.webmeets.com/files/papers/ISF/2012/182/Martin_RollingDice.pdf
            https://en.wikipedia.org/wiki/Calculating_demand_forecast_accuracy
         */
        double avg_est = 0, avg_true = 0;
        for (int l = 0; l < N; ++l) {
            for (int r = l; r < N; ++r) {
                long estimate = getQueryEstimateLR(windowLengths, l, r);
                long trueval = getQueryTruevalLR(l, r);
                assert estimate >= trueval;
                avg_est += Mvals[l][r] * estimate;
                avg_true += Mvals[l][r] * trueval;
            }
        }
        return (avg_est - avg_true) / avg_true;
    }

    public double getMeanRelativeError(List<Integer> windowLengths) {
        double mean = 0;
        for (int l = 0; l < N; ++l) {
            for (int r = l; r < N; ++r) {
                mean += Mvals[l][r] * getQueryRelativeErrorLR(windowLengths, l, r);
            }
        }
        return mean;
    }

    public double getMeanMAPE(List<Integer> windowLengths) {
        double mean = 0;
        for (int l = 0; l < N; ++l) {
            for (int r = l; r < N; ++r) {
                mean += Mvals[l][r] * getQueryMAPELR(windowLengths, l, r);
            }
        }
        return mean;
    }

    public double getCost(List<Integer> windowLengths) {
        return getWAPE(windowLengths);
    }
}
