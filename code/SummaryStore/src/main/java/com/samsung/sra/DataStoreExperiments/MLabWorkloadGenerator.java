package com.samsung.sra.DataStoreExperiments;

import com.moandjiezana.toml.Toml;
import com.samsung.sra.DataStoreExperiments.Workload.Query;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MLabWorkloadGenerator implements WorkloadGenerator {
    private static final Logger logger = LoggerFactory.getLogger(MLabWorkloadGenerator.class);

    private static final long[] IPs = {
            400851043L,  // 10M occurences in entire stream
            3482766751L, // 1M occurences
            1249724727L, // 100K
            1286406217L, // 10K
            1194699896L, // 1K
            1541583913L, // 100
            1566985853L, // 10
            1976935968L // 1
    };

    private final long Q;
    private final int cmsOpIndex;

    public MLabWorkloadGenerator(Toml conf) {
        this.Q = conf.getLong("queries-per-group");
        this.cmsOpIndex = conf.getLong("cms-operator-index").intValue();
    }

    @Override
    public Workload generate(long T0, long T1) {
        Random rand = new Random(0);
        Workload workload = new Workload();
        // Age/length classes will sample query ranges from [0, T1-T0]. We will add T0 below to compensate
        List<AgeLengthClass> alClasses = CalendarAgeLengths.getClasses(T1 - T0 + 1);
        for (int i = 0; i < IPs.length; ++i) {
            Object[] cmsParams = {IPs[i]}; // CMS lookup on the IP address IPs[i]
            for (AgeLengthClass alCls: alClasses) {
                String groupName = i + alCls.toString();
                logger.debug("Generating group {}", groupName);
                List<Query> groupQueries = new ArrayList<>();
                workload.put(groupName, groupQueries);
                for (int q = 0; q < Q; ++q) {
                    Pair<Long, Long> ageLength = alCls.sample(rand);
                    long age = T0 + ageLength.getFirst(), length = ageLength.getSecond();
                    long r = T1 - age, l = r - length + 1;
                    Query query = new Query(Query.Type.CMS, l, r, cmsOpIndex, cmsParams, 0L);
                    groupQueries.add(query);
                }
            }
        }
        return workload;
    }
}
