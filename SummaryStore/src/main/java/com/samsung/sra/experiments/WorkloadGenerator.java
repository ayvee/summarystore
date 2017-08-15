package com.samsung.sra.experiments;

public interface WorkloadGenerator {
    /* Implementors must define a constructor with signature Generator(Toml params). It will be called via reflection. */

    /** Generate a workload of queries in the time range [T0, T1] */
    Workload generate(long T0, long T1);
}
