package com.samsung.sra.DataStoreExperiments;

import com.moandjiezana.toml.Toml;

import java.util.ArrayList;
import java.util.List;

public class GoogleLandmarkWorkloadGenerator implements WorkloadGenerator {
    private final int operatorIdx;
    private final long threshold;
    private final long ticksPerS;

    public GoogleLandmarkWorkloadGenerator(Toml config) {
        this.operatorIdx = config.getLong("max-operator-index").intValue();
        this.threshold = config.getLong("threshold");
        this.ticksPerS = config.getLong("ticks-per-second", 1L);
    }

    @Override
    public Workload generate(long T0, long T1) {
        Workload workload = new Workload();
        List<Workload.Query> queries = new ArrayList<>();
        workload.put("max", queries);
        T0 = ((T0 + ticksPerS - 1) / ticksPerS) * ticksPerS; // round up to nearest second
        for (long t = T0; t + ticksPerS <= T1; t += ticksPerS) {
            queries.add(new Workload.Query(Workload.Query.Type.MAX_THRESH, t, t + ticksPerS, operatorIdx, new Object[]{threshold}));
        }
        return workload;
    }
}
