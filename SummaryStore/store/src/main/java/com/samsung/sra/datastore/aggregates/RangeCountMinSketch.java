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
package com.samsung.sra.datastore.aggregates;

import com.clearspring.analytics.stream.frequency.CountMinSketch;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RangeCountMinSketch {
    private final long N;
    private final int L;
    private List<CountMinSketch> sketches = new ArrayList<CountMinSketch>();

    public RangeCountMinSketch(long N) {// int depth, int width) {
        this.N = N;
        L = (int)Math.ceil(Math.log(N) / Math.log(2));
        int seed = 0;
        for (int l = 0; l <= L; ++l) {
            //sketches.add(new CountMinSketch(depth, width, seed));
            sketches.add(new CountMinSketch(0.01, 0.95, seed));
        }
    }

    private static long pow2(int k) {
        return 1L << k;
        /*long ret = 1;
        for (int j = 0; j < k; ++j) {
            ret *= 2;
        }
        return ret;*/
    }

    private static long getDyadicIndex(long n, int l) {
        return n / pow2(l);
    }

    public void add(long n, long count) {
        assert 0 <= n && n < N;
        for (int l = 0; l <= L; ++l) {
            long d = getDyadicIndex(n, l);
            //System.out.println("Dyadic index ["+n+","+l+"]:"+d);
            sketches.get(l).add(d, count);
        }
    }

    private long estimateCountRecursive(long t0, long t1) {
        if (t0 > t1) return 0;
        // find the largest dyadic interval starting at t0 contained in [t0, t1],
        // get its count, recurse on rest
        long base = 1;
        for (int k = 0; k <= L+1; ++k) {
            assert base == pow2(k);
            if (t0 % base == 0 && t0 + base - 1 <= t1) {
                base *= 2;
            } else {
                base /= 2;
                --k;
                System.out.println("decomp: [" + t0 + ", " + (t0 + base - 1) + "]; k = " + k + ", d = " + (t0 / base) + "; estimate = " + sketches.get(k).estimateCount(t0 / base));
                // return count[t0, t0 + base - 1] + count[t0 + base, t1]
                return sketches.get(k).estimateCount(t0 / base) + estimateCountRecursive(t0 + base, t1);
            }
        }
        throw new IllegalStateException("bottomed out of recursion");
    }

    public long estimateCount(long t0, long t1) {
        assert 0 <= t0 && t0 <= t1 && t1 < N;
        return estimateCountRecursive(t0, t1);
    }

    public static void main(String[] args) {
        long N = 10000; int U = 100000;
        RangeCountMinSketch sketch = new RangeCountMinSketch(N);
        //CountMinSketch sketch = new CountMinSketch(0.0001, 0.95, 0);
        Random random = new Random();
        int truesum = 0, t0 = 2, t1 = 63;
        for (long n = 0; n < N; ++n) {
            int val = random.nextInt(U);
            sketch.add(n, val);
            if (t0 <= n && n <= t1) truesum += val;
        }
        System.out.println("[" + t0 + ", " + t1 + "] -> true = " + truesum + "; est = " + sketch.estimateCount(t0, t1));
    }
}
