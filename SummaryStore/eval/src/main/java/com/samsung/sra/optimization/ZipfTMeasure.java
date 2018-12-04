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
