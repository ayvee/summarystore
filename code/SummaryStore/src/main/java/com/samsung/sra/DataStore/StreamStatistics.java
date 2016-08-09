package com.samsung.sra.DataStore;

import java.io.Serializable;

public class StreamStatistics implements Serializable {
    long firstArrivalTimestamp = -1, lastArrivalTimestamp = -1;
    long numValues = 0;
    private double Isum = 0, Isqsum = 0;
    private double Vsum = 0, Vsqsum = 0;

    public void append(long ts, Object value) {
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
