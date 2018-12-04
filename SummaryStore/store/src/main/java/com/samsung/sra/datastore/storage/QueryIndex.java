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
package com.samsung.sra.datastore.storage;

import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;

import java.io.Serializable;
import java.util.Collections;
import java.util.SortedSet;
import java.util.stream.Stream;

/** In-memory index over window time-starts, used to support query() */
class QueryIndex implements Serializable {
    private final SortedSet<Long> tStarts = Collections.synchronizedSortedSet(new LongAVLTreeSet());

    void add(long tStart) {
        tStarts.add(tStart);
    }

    void remove(long tStart) {
        tStarts.remove(tStart);
    }

    /**
     * Get windows that might overlap [t0, t1], specifically
     *     [last window with tStart < t0, ..., last window with tStart <= t1]
     * Very first window may not overlap [t0, t1], depending on its tEnd; should probably use this function as
     *    getOverlappingWindowIDs(t0, t1).map(windowGetter).filter(w -> w.te >= t0)
     */
    Stream<Long> getOverlappingWindowIDs(long t0, long t1) {
        if (tStarts.isEmpty()) return Stream.empty();
        // respectively, all windows with tStart < t0 and > t1
        SortedSet<Long> lt = tStarts.headSet(t0), gt = tStarts.tailSet(t1 + 1);
        long l = !lt.isEmpty() ? lt.last() : tStarts.first();
        long r = !gt.isEmpty() ? gt.first() : tStarts.last() + 1;
        return tStarts.subSet(l, r).stream();
    }

    long getNumWindows() {
        return tStarts.size();
    }
}
