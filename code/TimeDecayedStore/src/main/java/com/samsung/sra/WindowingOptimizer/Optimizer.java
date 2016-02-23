package com.samsung.sra.WindowingOptimizer;

/**
 * Implements the dynamic programming algo from
 * https://confluence.sisa.samsung.com:8443/display/summarystore/Time-decayed+aggregation
 * (see the page for documentation)
 */
public class Optimizer {
    private final int N;
    private final TMeasure P;
    private final EFunc e;

    public Optimizer(int N, TMeasure P, EFunc e) {
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
                    Mj[r][m] = P.M_l_r(0, m-1); // i.e. P[0][m-1]
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

    public void optimize(int W) {
        double[][] E = compute_E();
        double[][] C = new double[N+1][W+1];
        // right_endpoints[i][B] = j such that [i, j] is the first interval in an optimal B-byte covering of [i, N-1]
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
        System.out.print("intervals(" + W + ") = ");
        int i = 0;
        for (int B = W; B >= 1; --B) {
            int j = right_endpoints[i][B];
            //System.out.print(" [" + i + ", " + (j) + "]");
            System.out.print(" " + (j - i+1));
            i = j + 1;
        }
        System.out.println();
    }

    public static void main(String[] args) {
        int N = 1000;
        //Optimizer opt = new Optimizer(N, new UniformTMeasure(N), new LinearEFunc());
        Optimizer opt = new Optimizer(N, new ZipfTMeasure(N, 0.5), new LinearEFunc());
        opt.optimize(10);
        opt.optimize(20);
        opt.optimize(21);
        opt.optimize(50);
        opt.optimize(100);
    }
}
