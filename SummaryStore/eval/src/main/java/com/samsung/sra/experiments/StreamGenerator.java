/*
* Copyright 2016 Samsung Research America. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.samsung.sra.experiments;

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

    default boolean isCopyable() {
        return false;
    }

    default StreamGenerator copy() {
        return null;
    }

    class Operation {
        public enum Type {
            APPEND,
            LANDMARK_START,
            LANDMARK_END
        }

        Type type;
        long timestamp;
        Object value;

        public Operation(Type type, long timestamp, Object value) {
            this.type = type;
            this.timestamp = timestamp;
            this.value = value;
        }
    }
}
