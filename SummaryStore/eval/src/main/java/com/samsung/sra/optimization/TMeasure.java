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
 * Essentially a probability distribution over all time intervals contained in [0, N-1].
 * We call it a measure instead of a probability distribution because we don't insist on
 * normalization: \sum_[l, r] M[l, r] doesn't have to equal 1.
 *
 * Subclasses must override at least one of M_l_r and M_a_l.
 */
public abstract class TMeasure {
    public final int N;

    protected TMeasure(int N) {
        this.N = N;
    }

    /**
     * What is the measure of the interval [l, r]?
     */
    public double M_l_r(int l, int r) {
        return M_a_l(N - 1 - r, r - l + 1);
    }

    /**
     * What is the measure of the interval whose right endpoint has age a
     * and whose length is l?
     */
    public double M_a_l(int a, int l) {
        return M_l_r(N - l - a, N - 1 - a);
    }
}
