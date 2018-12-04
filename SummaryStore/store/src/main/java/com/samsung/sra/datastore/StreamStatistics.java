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

public class StreamStatistics implements Serializable {
    private long firstArrivalTimestamp = -1, lastArrivalTimestamp = -1;
    private long numValues = 0;
    private double Isum = 0, Isqsum = 0;
    private double Vsum = 0, Vsqsum = 0;

    public StreamStatistics() {
    }

    public StreamStatistics(StreamStatistics that) {
        this.firstArrivalTimestamp = that.firstArrivalTimestamp;
        this.lastArrivalTimestamp = that.lastArrivalTimestamp;
        this.numValues = that.numValues;
        this.Isum = that.Isum;
        this.Isqsum = that.Isqsum;
        this.Vsum = that.Vsum;
        this.Vsqsum = that.Vsqsum;
    }

    void append(long ts, Object value) {
        assert ts > lastArrivalTimestamp;
        if (firstArrivalTimestamp == -1) {
            firstArrivalTimestamp = ts;
        } else {
            double I = ts - lastArrivalTimestamp;
            Isum += I;
            Isqsum += I * I;
        }
        if (value instanceof Number) {
            double v = ((Number) value).doubleValue();
            Vsum += v;
            Vsqsum += v * v;
        }
        ++numValues;
        lastArrivalTimestamp = ts;
    }

    public long getTimeRangeStart() {
        return firstArrivalTimestamp;
    }

    public long getTimeRangeEnd() {
        return lastArrivalTimestamp;
    }

    public long getNumValues() {
        return numValues;
    }

    public double getMeanInterarrival() {
        // note that # interarrivals = numValues - 1
        return numValues > 1 ? Isum / (numValues - 1) : 0;
    }

    public double getSDInterarrival() {
        return numValues > 2 ? Math.sqrt((Isqsum - Isum * Isum / (numValues - 1d)) / (numValues - 2d)) : 0;
    }

    public double getCVInterarrival() {
        return getSDInterarrival() / getMeanInterarrival();
    }

    /** WARNING: silently returns 0 in non-numeric streams */
    public double getMeanValue() {
        return numValues > 0 ? Vsum / numValues : 0;
    }

    /** WARNING: silently returns 0 in non-numeric streams */
    public double getSDValue() {
        return numValues > 1 ? Math.sqrt((Vsqsum - Vsum * Vsum / numValues) / (numValues - 1d)) : 0;
    }

    /** WARNING: undefined behavior in non-numeric streams */
    public double getCVValue() {
        return getSDValue() / getMeanValue();
    }
}
