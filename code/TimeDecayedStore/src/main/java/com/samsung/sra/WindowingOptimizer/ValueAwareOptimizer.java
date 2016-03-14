package com.samsung.sra.WindowingOptimizer;

import javafx.util.converter.LocalDateTimeStringConverter;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Implements the dynamic programming algo from
 * https://confluence.sisa.samsung.com:8443/display/summarystore/Time-decayed+aggregation
 * (see the page for documentation)
 */
public class ValueAwareOptimizer {
    private final int N;
    private final TMeasure P;
    private final long[] counts;

    public ValueAwareOptimizer(int N, TMeasure P, long[] counts) {
        assert counts.length == N;
        this.N = N;
        this.P = P;
        this.counts = counts;
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
        // https://en.wikipedia.org/wiki/Symmetric_mean_absolute_percentage_error
        long nr = estimate - trueval, dr = estimate + trueval;
        return dr > 0 ? nr / (double)dr : 0;
    }

    private void compute_E(double[][] E, double[][] Ml, double[][] Mr, double[][] Mvals, long[][] trueCounts) {
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
            double[][] Ml = new double[N][N], Mr = new double[N][N], Mvals = new double[N][N];
            long[][] trueCounts = new long[N][N];
            compute_Ml(Ml);
            compute_Mr(Mr);
            compute_Mvals(Mvals);
            compute_trueCounts(trueCounts);
            compute_E(E, Ml, Mr, Mvals, trueCounts);
        }
        return E;
    }

    public double optimize(int W) {
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
        /*List<Integer> lengths = new ArrayList<Integer>();
        int i = 0;
        for (int B = W; B >= 1; --B) {
            int j = right_endpoints[i][B];
            lengths.add(j - i + 1);
            i = j + 1;
        }
        assert Math.abs(C[0][W] - getCost(lengths)) < 1e-5;
        //return lengths;*/
        return C[0][W];
    }

    public double getCost(Integer[] windowLengths) {
        return getCost(Arrays.asList(windowLengths));
    }

    public double getCost(List<Integer> windowLengths) {
        double[][] E = compute_E();
        double cost = 0;
        int i = 0;
        for (int l: windowLengths) {
            int j = i + l - 1;
            cost += E[i][j];
            i = j + 1;
        }
        if (i != N) {
            throw new IllegalArgumentException("window lengths must sum to N = " + N);
        }
        return cost;
    }
}
