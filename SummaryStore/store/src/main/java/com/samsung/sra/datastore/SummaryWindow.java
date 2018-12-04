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

/**
 * Window holding a set of summary data structures. SummaryStore stream = list of contiguous SummaryWindows
 *
 * Dumb structs. All the code creating, manipulating, and querying windows is in Stream */
public class SummaryWindow implements Serializable {
    // metadata
    /* We use longs for window IDs, timestamps, and count markers. Valid values should be
       non-negative (all three are 0-indexed); use "-1" to indicate null values. */
    public long ts, te, cs, ce;

    // data
    public Object[] aggregates;

    public SummaryWindow() {}

    public SummaryWindow(WindowOperator[] operators, long ts, long te, long cs, long ce) {
        this.ts = ts;
        this.te = te;
        this.cs = cs;
        this.ce = ce;
        aggregates = new Object[operators.length];
        for (int i = 0; i < aggregates.length; ++i) {
            aggregates[i] = operators[i].createEmpty(); // empty aggr
        }
    }


    @Override
    public String toString() {
        StringBuilder ret = new StringBuilder(String.format(
                "<summary-window: time range [%d:%d], count range [%d:%d], aggrs [", ts, te, cs, ce));
        boolean first = true;
        for (Object aggregate : aggregates) {
            if (first) {
                first = false;
            } else {
                ret.append(", ");
            }
            ret.append(aggregate);
        }
        ret.append("]>");
        return ret.toString();
    }

}
