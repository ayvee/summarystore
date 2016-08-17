package com.samsung.sra.DataStoreExperiments;

import java.io.IOException;
import java.util.function.BiConsumer;

public interface StreamGenerator extends AutoCloseable {
    void generate(long T, BiConsumer<Long, Long> consumer) throws IOException;

    void reset() throws IOException;

    default void close() throws Exception {}
}
