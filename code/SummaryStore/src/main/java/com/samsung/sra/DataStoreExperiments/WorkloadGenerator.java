package com.samsung.sra.DataStoreExperiments;

public interface WorkloadGenerator<R> {
    /** Generate a workload of queries in the time range [0, T] */
    Workload<R> generate(long T);
}
