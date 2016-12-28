package com.samsung.sra.DataStoreExperiments;

import java.io.IOException;
import java.util.function.BiConsumer;

public interface StreamGenerator extends AutoCloseable {
    /* Implementors must define a constructor with signature Generator(Toml params). It will be called via reflection. */

    /** Generate data points spanning [T0, T1] */
    void generate(long T0, long T1, BiConsumer<Long, Object[]> consumer) throws IOException;

    /** Calling generate with the same T after reset should yield the exact same time series again */
    void reset() throws IOException;

    default void close() throws Exception {}
}
