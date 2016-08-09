package com.samsung.sra.DataStoreExperiments;

import java.util.function.BiConsumer;

public interface StreamGenerator {
    void generate(long T, BiConsumer<Long, Long> consumer);

    void reset();
}
