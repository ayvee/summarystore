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

import com.samsung.sra.experiments.Distribution;

import java.util.SplittableRandom;

public class BinnedStreamGenerator {
    public static long[] generateBinnedStream(int T, Distribution<Long> interarrivals) {
        assert T >= 1;
        long[] ret = new long[T];
        double t = 0;
        SplittableRandom random = new SplittableRandom(0);
        while (true) {
            t += interarrivals.next(random);
            if ((int)t >= T) {
                break;
            } else {
                ret[(int)t] += 1;
            }
        }
        return ret;
    }
}
