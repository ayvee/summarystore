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

public class UniformDistribution implements Distribution<Long> {
    private final long min, max;

    public UniformDistribution(Toml conf) {
        this.min = conf.getLong("min");
        this.max = conf.getLong("max");
        assert min <= max;
    }

    @Override
    public Long next(SplittableRandom random) {
        return min + Math.abs(random.nextLong()) % (max - min + 1);
    }
}
