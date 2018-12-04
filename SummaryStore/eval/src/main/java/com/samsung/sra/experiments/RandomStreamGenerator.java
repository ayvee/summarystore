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
import java.util.function.Consumer;

public class RandomStreamGenerator implements StreamGenerator {
    private final Distribution<Long> interarrivals, values;
    private SplittableRandom random;
    private final long R;

    public RandomStreamGenerator(Toml params) {
        this.interarrivals = Configuration.parseDistribution(params.getTable("interarrivals"));
        this.values = Configuration.parseDistribution(params.getTable("values"));
        this.R = params.getLong("random-seed", 0L);
        this.random = new SplittableRandom(R);
    }

    @Override
    public void generate(long T0, long T1, Consumer<Operation> consumer) {
        for (long t = T0; t <= T1; t += interarrivals.next(random)) {
            Object v = values.next(random);
            consumer.accept(new Operation(Operation.Type.APPEND, t, v));
        }
    }

    @Override
    public void reset() {
        random = new SplittableRandom(R);
    }
}
