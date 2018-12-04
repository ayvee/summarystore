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
package com.samsung.sra.datastore;

import java.io.Serializable;
import java.util.SortedMap;
import java.util.TreeMap;

public class LandmarkWindow implements Serializable {
    public long ts, te;
    public final SortedMap<Long, Object> values = new TreeMap<>();

    public LandmarkWindow(long ts) {
        this.ts = ts;
    }

    public void append(long timestamp, Object value) {
        assert timestamp >= ts && (values.isEmpty() || values.lastKey() < timestamp);
        values.put(timestamp, value);
    }

    public void close(long timestamp) {
        assert timestamp >= ts && (values.isEmpty() || values.lastKey() <= timestamp);
        te = timestamp;
    }

    @Override
    public String toString() {
        return String.format("<landmark-window: time range [%d:%d], %d values", ts, te, values.size());
    }
}
