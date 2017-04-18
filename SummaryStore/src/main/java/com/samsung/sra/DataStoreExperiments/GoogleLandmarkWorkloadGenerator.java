package com.samsung.sra.DataStoreExperiments;

import com.moandjiezana.toml.Toml;
import com.samsung.sra.DataStoreExperiments.Workload.Query;

import java.util.ArrayList;
import java.util.List;

public class GoogleLandmarkWorkloadGenerator implements WorkloadGenerator {
    private final int maxOpIdx, sumOpIdx, countOpIdx;
    private final long threshold;
    private final long ticksPerS;
    private final String queryType;
    private final boolean isMaxQuery; // is mean query if false

    public GoogleLandmarkWorkloadGenerator(Toml config) {
        String query = config.getString("query").toLowerCase();
        assert query.equals("max") || query.equals("mean");
        this.queryType = query;
        this.isMaxQuery = query.equals("max");
        this.maxOpIdx = config.getLong("max-operator-index", -1L).intValue();
        this.sumOpIdx = config.getLong("sum-operator-index", -1L).intValue();
        this.countOpIdx = config.getLong("count-operator-index", -1L).intValue();
        this.threshold = config.getLong("threshold");
        this.ticksPerS = config.getLong("ticks-per-second", 1L);
    }

    @Override
    public Workload generate(long T0, long T1) {
        Workload workload = new Workload();
        List<Query> queries = new ArrayList<>();
        workload.put(queryType, queries);
        T0 = ((T0 + ticksPerS - 1) / ticksPerS) * ticksPerS; // round up to nearest second
        Query.Type queryType = isMaxQuery ? Query.Type.MAX_THRESH : Query.Type.MEAN;
        Object[] queryParams = isMaxQuery ? new Object[]{threshold} : new Object[]{sumOpIdx, countOpIdx};
        for (long t = T0; t + ticksPerS <= T1; t += ticksPerS) {
            // NOTE: mean query ignores operator index maxOpIdx
            queries.add(new Query(queryType, t, t + ticksPerS, maxOpIdx, queryParams));
        }
        return workload;
    }
}
