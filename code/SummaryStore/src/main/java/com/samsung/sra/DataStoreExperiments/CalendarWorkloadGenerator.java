package com.samsung.sra.DataStoreExperiments;

import com.moandjiezana.toml.Toml;
import com.samsung.sra.DataStoreExperiments.Workload.Query;
import org.apache.commons.math3.util.Pair;

import java.util.*;

/** Generate a workload of random count/sum/(TODO) queries with calendar age/lengths. */
public class CalendarWorkloadGenerator implements WorkloadGenerator {
    private final int operatorIndex;
    private final Query.Type operatorType;
    private final Distribution<Long> cmsParamDistr;
    private final long ticksPerS;
    private final long Q;

    public CalendarWorkloadGenerator(Toml conf) {
        operatorIndex = conf.getLong("operator.index").intValue();
        operatorType = Query.Type.valueOf(conf.getString("operator.type").toUpperCase());
        Toml cmsParamSpec = conf.getTable("operator.param");
        cmsParamDistr = cmsParamSpec != null
                ? Configuration.parseDistribution(cmsParamSpec)
                : null;
        Q = conf.getLong("queries-per-group");
        ticksPerS = conf.getLong("ticks-per-second", 1L);
    }

    @Override
    public Workload generate(long T0, long T1) {
        Random rand = new Random(0);
        Workload workload = new Workload();
        // Age/length classes will sample query ranges from [0s, (T1-T0) in seconds]. We will rescale below to correct
        List<AgeLengthClass> alClasses = CalendarAgeLengths.getClasses((T1 - T0) / ticksPerS);
        for (AgeLengthClass alCls : alClasses) {
            String groupName = String.format("%s\t%s", operatorType, alCls.toString());
            List<Query> groupQueries = new ArrayList<>();
            workload.put(groupName, groupQueries);
            for (int q = 0; q < Q; ++q) {
                Pair<Long, Long> ageLength = alCls.sample(rand); // both in seconds
                long age = ageLength.getFirst() * ticksPerS, length = ageLength.getSecond() * ticksPerS;
                long r = T1 - age, l = r - length + ticksPerS;
                assert T0 <= l && l <= r && r <= T1 :
                        String.format("[T0, T1] = [%s, %s], age = %s, length = %s, [l, r] = [%s, %s]",
                                T0, T1, age, length, l, r);
                Query query;
                switch (operatorType) {
                    case COUNT:
                        query = new Query(Query.Type.COUNT, l, r, operatorIndex, null, 0L);
                        break;
                    case SUM:
                        query = new Query(Query.Type.SUM, l, r, operatorIndex, null, 0L);
                        break;
                    case CMS:
                        assert cmsParamDistr != null;
                        query = new Query(Query.Type.CMS, l, r, operatorIndex, new Object[]{cmsParamDistr.next(rand)}, 0L);
                        break;
                    default:
                        throw new IllegalStateException("hit unreachable code");
                }
                groupQueries.add(query);
            }
        }
        return workload;
    }
}
