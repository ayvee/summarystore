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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MLabWorkloadGenerator implements WorkloadGenerator {
    private static final Logger logger = LoggerFactory.getLogger(MLabWorkloadGenerator.class);

    private static final long[] clientIPs = {
            400851043L,  // 10M occurrences in entire stream
            3482766751L, // 1M occurrences
            1249724727L, // 100K
            1286406217L, // 10K
            1194699896L, // 1K
            1541583913L, // 100
            1566985853L, // 10
            1976935968L // 1
    };

    private static final long[] serverIPs = {
            643468428L,  // 2M occurrences in entire stream
            644499116L,  // 1M occurrences
            69351948L,   // 100K
            1131075532L, // 10K
            1093545638L, // 1K
            3162013449L, // 100
            1023933462L, // 10
            69463753L    // 1
    };

    private final long[] IPs;

    private final int cmsOpIndex;

    /** Example config.toml spec:
     *
     * [workload]
     * workload-generator = "MLabWorkloadGenerator"
     * cms-operator-index = 0
     */
    public MLabWorkloadGenerator(Toml conf) {
        this.cmsOpIndex = conf.getLong("cms-operator-index").intValue();
        switch (conf.getString("mode").toLowerCase()) {
            case "server":
                IPs = serverIPs;
                break;
            case "client":
                IPs = clientIPs;
                break;
            default:
                throw new IllegalArgumentException("unknown mode");
        }
    }

    @Override
    public Workload generate(long T0, long T1) {
        Workload workload = new Workload();
        // Age/length classes will sample query ranges from [0s, (T1-T0) in seconds]. We will rescale below to correct
        List<AgeLengthClass> alClasses = CalendarAgeLengths.getClasses((T1 - T0) / (long)1e9); // MLab is in nanoseconds
        for (int i = 0; i < IPs.length; ++i) {
            Object[] cmsParams = {IPs[i]}; // CMS lookup on the IP address IPs[i]
            for (AgeLengthClass alCls: alClasses) {
                String groupName = String.format("Q%d\t%s", i, alCls);
                logger.debug("Generating group {}", groupName);
                List<Query> groupQueries = new ArrayList<>();
                workload.put(groupName, groupQueries);
                for (Pair<Long, Long> ageLength: alCls.getAllAgeLengths()) {
                    // pair is in seconds, need to convert to ns first
                    long age = ageLength.getFirst() * (long)1e9, length = ageLength.getSecond() * (long)1e9;
                    //assert 0 <= age && age <= T1-T0 && 0 <= length && length <= T1-T0;
                    long r = T1 - age, l = r - length + (long)1e9;
                    assert T0 <= l && l <= r && r <= T1 :
                            String.format("[T0, T1] = [%s, %s], age = %s, length = %s, [l, r] = [%s, %s]",
                                    T0, T1, age, length, l, r);
                    Query query = new Query(Query.Type.CMS, l, r, cmsOpIndex, cmsParams);
                    groupQueries.add(query);
                }
            }
        }
        return workload;
    }
}
