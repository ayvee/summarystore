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
import com.samsung.sra.experiments.Workload.Query;

import java.util.ArrayList;
import java.util.List;

public class GoogleSummaryWorkloadGenerator implements WorkloadGenerator {
    private final int sumOpIdx, countOpIdx;
    private final long binSize;
    private final long ticksPerS;

    public GoogleSummaryWorkloadGenerator(Toml config) {
        this.sumOpIdx = config.getLong("sum-operator-index", -1L).intValue();
        this.countOpIdx = config.getLong("count-operator-index", -1L).intValue();
        this.ticksPerS = config.getLong("ticks-per-second", 1L);
        this.binSize = config.getLong("seconds-per-query", 3600L) * ticksPerS;
    }

    @Override
    public Workload generate(long T0, long T1) {
        Workload workload = new Workload();
        List<Query> sumQueries = new ArrayList<>(), countQueries = new ArrayList<>();
        workload.put("sum", sumQueries);
        workload.put("count", countQueries);
        for (long t = T1; t - binSize >= T0; t -= binSize) {
            sumQueries.add(new Query(Query.Type.SUM, t - binSize, t, sumOpIdx, null));
            countQueries.add(new Query(Query.Type.COUNT, t - binSize, t, sumOpIdx, null));
        }
        return workload;
    }
}
