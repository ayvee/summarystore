package com.samsung.sra.DataStoreExperiments;

import com.moandjiezana.toml.Toml;
import com.samsung.sra.DataStoreExperiments.Workload.Query;
import org.apache.commons.math3.util.Pair;

import java.util.*;

public class TSMWorkloadGenerator implements WorkloadGenerator {
    private final Long countIdx, sumIdx, cmsIdx; // indices of count/sum/CMS operators
    private final Long numNodes;
    private final long Q;

    public TSMWorkloadGenerator(Toml conf) {
        countIdx = conf.getLong("count-operator-index", null);
        sumIdx = conf.getLong("sum-operator-index", null);
        cmsIdx = conf.getLong("cms-operator-index", null);
        numNodes = conf.getLong("num-nodes", null);
        Q = conf.getLong("queries-per-group");
    }

    /**
     * Generates upto three types of queries: COUNT, SUM, CMS(uniform random nodeID). Specify an operator index in the
     * constructor to enable each query type.
     *
     * For each query type, generates calendar age/length bins.
     */
    @Override
    public Workload generate(long T0, long T1) {
        Random rand = new Random(0);
        Workload workload = new Workload();
        List<Query.Type> queryTypes = new ArrayList<>();
        if (countIdx != null) {
            queryTypes.add(Query.Type.COUNT);
        }
        if (sumIdx != null) {
            queryTypes.add(Query.Type.SUM);
        }
        if (cmsIdx != null) {
            queryTypes.add(Query.Type.CMS);
        }
        assert !queryTypes.isEmpty();
        // Age/length classes will sample query ranges from [0s, (T1-T0) in seconds]. We will rescale below to correct
        List<AgeLengthClass> alClasses = CalendarAgeLengths.getClasses((T1 - T0) / (long)1e9); // TSM is in nanoseconds
        for (Query.Type queryType: queryTypes) {
            for (AgeLengthClass alCls: alClasses) {
                String groupName = String.format("%s\t%s", queryType, alCls.toString());
                List<Query> groupQueries = new ArrayList<>();
                workload.put(groupName, groupQueries);
                for (int q = 0; q < Q; ++q) {
                    Pair<Long, Long> ageLength = alCls.sample(rand); // both in seconds, we need to convert to ns
                    long age = ageLength.getFirst() * (long)1e9, length = ageLength.getSecond() * (long)1e9;
                    long r = T1 - age, l = r - length + (long)1e9;
                    assert T0 <= l && l <= r && r <= T1 :
                            String.format("[T0, T1] = [%s, %s], age = %s, length = %s, [l, r] = [%s, %s]",
                                    T0, T1, age, length, l, r);
                    Query query;
                    switch (queryType) {
                        case COUNT:
                            assert countIdx != null;
                            query = new Query(Query.Type.COUNT, l, r, countIdx.intValue(), null, 0L);
                            break;
                        case SUM:
                            assert sumIdx != null;
                            query = new Query(Query.Type.SUM, l, r, sumIdx.intValue(), null, 0L);
                            break;
                        case CMS:
                            assert cmsIdx != null && numNodes != null;
                            long nodeID = Math.abs(rand.nextLong()) % numNodes;
                            query = new Query(Query.Type.CMS, l, r, cmsIdx.intValue(), new Object[]{nodeID}, 0L);
                            break;
                        default:
                            throw new IllegalStateException("hit unreachable code");
                    }
                    groupQueries.add(query);
                }
            }
        }
        return workload;
    }
}
