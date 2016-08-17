package com.samsung.sra.DataStoreExperiments;

import java.io.IOException;
import java.util.function.BiConsumer;

public interface StreamGenerator extends AutoCloseable {
    /** Generate data points spanning [0, T] */
    void generate(long T, BiConsumer<Long, Long> consumer) throws IOException;

    /** Calling generate with the same T after reset should yield the exact same time series again */
    void reset() throws IOException;

    default void close() throws Exception {}
}
