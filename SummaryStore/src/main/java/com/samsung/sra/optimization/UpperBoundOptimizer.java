package com.samsung.sra.optimization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Find the optimal decay function for a given "workload" (a distribution over queries).
 *
 * Implements the dynamic programming algo from
 * https://confluence.sisa.samsung.com:8443/display/summarystore/Time-decayed+aggregation
 * (see the page for documentation)
 */
public class UpperBoundOptimizer {
    private final int N;
    private final TMeasure P;
    private final EFunc e;

    public UpperBoundOptimizer(int N, TMeasure P, EFunc e) {
        this.N = N;
        this.P = P;
        this.e = e;
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

    private void compute_Mj(double[][] Mj) {
        for (int m = 1; m <= N; ++m) {
            for (int r = 0; r < N; ++r) {
                if (r < m - 1) {
                    Mj[r][m] = 0;
                } else if (r == m - 1){
                    Mj[r][m] = P.M_l_r(0, m-1);
                } else {
                    Mj[r][m] = Mj[r-1][m] + P.M_l_r(r - m + 1, r);
                }
            }
        }
    }

    private void compute_E(double[][] E, double[][] Ml, double[][] Mr, double[][] Mj) {
        for (int l = 0; l < N; ++l) {
            for (int r = 0; r < N; ++r) {
                int d = r - l + 1;
                E[l][r] = 0;
                for (int m = 1; m <= d; ++m) {
                    E[l][r] += e.e(d, m) * (
                              Ml[l][l + m - 1]
                            + Mr[l + d - m][l + d - 1]
                            + Mj[l + d - 1][m]
                            - (l + m - 2 >= 0 ? Mj[l + m - 2][m] : 0));
                }
            }
        }
    }

    private double[][] E = null;

    private double[][] compute_E() {
        if (E == null) {
            E = new double[N][N];
            double[][] Ml = new double[N][N], Mr = new double[N][N], Mj = new double[N][N + 1];
            compute_Ml(Ml);
            compute_Mr(Mr);
            compute_Mj(Mj);
            compute_E(E, Ml, Mr, Mj);
        }
        return E;
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
        assert Math.abs(C[0][W] - getCost(lengths)) < 1e-5;
        return lengths;
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

    public double getIntervalWeight(int l, int r) {
        double Psum = 0;
        for (int p = 0; p <= r; ++p) {
            for (int q = Math.max(p, l); q < N; ++q) {
                Psum += P.M_l_r(p, q);
            }
        }
        return Psum;
    }

    public static void main(String[] args) {
        int N = 1023;
        int W = 10;
        Integer[] linearLengths = {103, 103, 103, 102, 102, 102, 102, 102, 102, 102};
        Integer[] exponentialLengths = {512, 256, 128, 64, 32, 16, 8, 4, 2, 1};
        double[] svals = {1e-6, 0.25, 0.5, 0.75, 1, 1.25, 1.5, 1.75, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        //System.out.println("#s\tlength ratio\tZipf P(N>9)");
        System.out.println("#s\topt cost\tlinearstore cost\tpow(2) EH cost\topt length ratio");
        for (double s: svals) {
            UpperBoundOptimizer opt = new UpperBoundOptimizer(N, new ZipfTMeasure(N, s), new LinearEFunc());
            List<Integer> optimalLengths = opt.optimize(W);
            double optCost = opt.getCost(optimalLengths);
            double linearCost = opt.getCost(linearLengths);
            double exponentialCost = opt.getCost(exponentialLengths);
            double optRatio = optimalLengths.get(0) / (double)optimalLengths.get(W-1);
            System.out.println(s + "\t" + optCost + "\t" + linearCost + "\t" + exponentialCost + "\t" + optRatio);
            System.err.print("intervals(" + s + ") =");
            int i = 0;
            for (int length: optimalLengths) {
                //System.err.print(" " + length);
                System.err.print(" " + opt.getIntervalWeight(i, i + length - 1));
                i += length;
            }
            System.err.println();
        }
    }
}
