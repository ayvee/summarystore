package com.samsung.sra.DataStoreExperiments;

import com.moandjiezana.toml.Toml;
import com.samsung.sra.DataStoreExperiments.Workload.Query;

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
