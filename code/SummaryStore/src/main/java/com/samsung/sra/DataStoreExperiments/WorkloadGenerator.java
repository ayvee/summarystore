package com.samsung.sra.DataStoreExperiments;

public interface WorkloadGenerator<R> {
    /* Implementors must define a constructor with signature Generator(Toml params). It will be called via reflection. */

    /** Generate a workload of queries in the time range [0, T] */
    Workload generate(long T);
}
