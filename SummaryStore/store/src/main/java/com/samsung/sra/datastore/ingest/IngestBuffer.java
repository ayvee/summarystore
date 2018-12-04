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
package com.samsung.sra.datastore.ingest;

import java.io.Serializable;

/**
 * Fixed-size buffer of time-value pairs
 */
interface IngestBuffer extends AutoCloseable, Serializable {
    void append(long ts, Object value);

    boolean isFull();

    int size();

    /**
     * Remove first s elements. After truncate
     *    values[0], values[1], ..., values[size - s - 1] = old values[s], values[s + 1], ..., values[size - 1]*/
    void truncateHead(int s);

    void clear();

    long getTimestamp(int pos);

    Object getValue(int pos);

    @Override
    default void close() {}
}
