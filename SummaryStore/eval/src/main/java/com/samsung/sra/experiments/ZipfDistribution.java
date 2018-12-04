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
package com.samsung.sra.experiments;

import com.moandjiezana.toml.Toml;

import java.util.SplittableRandom;

/** Zipf distribution over [1, N] with shape parameter k */
public class ZipfDistribution implements Distribution<Long> {
    private final org.apache.commons.math3.distribution.ZipfDistribution apacheZD;

    public ZipfDistribution(Toml conf) {
        int N = conf.getLong("N").intValue(); // range = [1, N]
        double k = conf.getDouble("k");
        apacheZD = new org.apache.commons.math3.distribution.ZipfDistribution(N, k);
    }

    /** WARNING: ignores argument and uses private RNG */
    @Override
    public Long next(SplittableRandom random) {
        return (long) apacheZD.sample();
    }
}
