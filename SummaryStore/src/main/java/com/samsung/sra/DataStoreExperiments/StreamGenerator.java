package com.samsung.sra.DataStoreExperiments;

import java.io.IOException;
import java.util.function.Consumer;

/** Generate one time series stream, as a sequence of append-value/start-landmark/end-landmark events */
public interface StreamGenerator extends AutoCloseable {
    /* Implementors must define a constructor with signature Generator(Toml params). It will be called via reflection. */

    /** Generate data points spanning [T0, T1] */
    void generate(long T0, long T1, Consumer<Operation> consumer) throws IOException;

    /** Calling generate with the same T after reset should yield the exact same time series again */
    void reset() throws IOException;

    default void close() throws Exception {}

    class Operation {
        public enum Type {
            APPEND,
            LANDMARK_START,
            LANDMARK_END
        }

        Type type;
        long timestamp;
        Object[] value;

        public Operation(Type type, long timestamp, Object[] value) {
            this.type = type;
            this.timestamp = timestamp;
            this.value = value;
        }
    }
}
