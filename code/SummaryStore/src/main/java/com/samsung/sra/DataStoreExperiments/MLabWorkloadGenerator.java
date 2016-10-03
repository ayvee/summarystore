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

    /** Example config.toml spec:
     *
     * [workload]
     * workload-generator = "MLabWorkloadGenerator"
     * queries-per-group = 1000
     * cms-operator-index = 0
     */
    public MLabWorkloadGenerator(Toml conf) {
        this.Q = conf.getLong("queries-per-group");
        this.cmsOpIndex = conf.getLong("cms-operator-index").intValue();
    }

    @Override
    public Workload generate(long T0, long T1) {
        Random rand = new Random(0);
        Workload workload = new Workload();
        // Age/length classes will sample query ranges from [0s, (T1-T0) in seconds]. We will rescale below to correct
        List<AgeLengthClass> alClasses = CalendarAgeLengths.getClasses((T1 - T0) / (long)1e9); // MLab is in nanoseconds
        for (int i = 0; i < IPs.length; ++i) {
            Object[] cmsParams = {IPs[i]}; // CMS lookup on the IP address IPs[i]
            for (AgeLengthClass alCls: alClasses) {
                String groupName = i + alCls.toString();
                logger.debug("Generating group {}", groupName);
                List<Query> groupQueries = new ArrayList<>();
                workload.put(groupName, groupQueries);
                for (int q = 0; q < Q; ++q) {
                    Pair<Long, Long> ageLength = alCls.sample(rand); // both in seconds, we need to convert to ns
                    long age = ageLength.getFirst() * (long)1e9, length = ageLength.getSecond() * (long)1e9;
                    //assert 0 <= age && age <= T1-T0 && 0 <= length && length <= T1-T0;
                    long r = T1 - age, l = r - length + (long)1e9;
                    assert T0 <= l && l <= r && r <= T1 :
                            String.format("[T0, T1] = [%s, %s], age = %s, length = %s, [l, r] = [%s, %s]",
                                    T0, T1, age, length, l, r);
                    Query query = new Query(Query.Type.CMS, l, r, cmsOpIndex, cmsParams, 0L);
                    groupQueries.add(query);
                }
            }
        }
        return workload;
    }
}
