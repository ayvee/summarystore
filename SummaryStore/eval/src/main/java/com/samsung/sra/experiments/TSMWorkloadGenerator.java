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
import org.apache.commons.math3.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class TSMWorkloadGenerator implements WorkloadGenerator {
    private static final int countOp = 0, sumOp = 1;
    private static final long numDays = 365;

    private static final long ticksPerS = 1_000_000_000L;

    private static final long oneday = 86400L * ticksPerS;

    private static long dayStart(long T0, long day) {
        assert 0 <= day && day < 365;
        return T0 + day * oneday;
    }

    public TSMWorkloadGenerator(Toml params) {}

    @Override
    public Workload generate(long T0, long T1) {
        //assert dayStart(T0, 364) < T1 && T1 < dayStart(T0, 365);
        long maxAgeInSeconds = (T1 - T0) / ticksPerS;
        Workload workload = new Workload();
        /*workload.put("Q1\tsubsecond\tweeks", Collections.singletonList(
                    new Query(Query.Type.SUM, dayStart(T0, 358), T1, sumOp, null)));
        workload.put("Q6\tsubsecond\tdays", Collections.singletonList(
                new Query(Query.Type.SUM, dayStart(T0, 364), T1, sumOp, null)));*/
        {
            AgeLengthClass.Bin lengthBin = new AgeLengthClass.Bin("days", 86400, 86400, 1); // length is exactly one day
            for (AgeLengthClass.Bin ageBin: CalendarAgeLengths.getAllBins()) {
                if (ageBin.getStart() + lengthBin.getStart() - 1 <= maxAgeInSeconds) {
                    AgeLengthClass alClass = new AgeLengthClass(ageBin, lengthBin, maxAgeInSeconds);
                    List<Query> queries = new ArrayList<>();
                    workload.put("Q7\t" + alClass.toString(), queries);
                    for (Pair<Long, Long> ageLength: alClass.getAllAgeLengths()) {
                        long age = ageLength.getFirst() * ticksPerS, length = ageLength.getSecond() * ticksPerS;
                        long r = T1 - age, l = r - length + ticksPerS;
                        queries.add(new Query(Query.Type.SUM, l, r, sumOp, null));
                    }
                }
            }
        }
        return workload;
    }
}
