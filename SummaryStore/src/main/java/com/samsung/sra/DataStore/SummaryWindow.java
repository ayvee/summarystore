package com.samsung.sra.DataStore;

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
    public long prevTS, nextTS;

    // data
    public Object[] aggregates;

    public SummaryWindow() {}

    public SummaryWindow(WindowOperator[] operators, long ts, long te, long cs, long ce, long prevTS, long nextTS) {
        this.ts = ts;
        this.te = te;
        this.cs = cs;
        this.ce = ce;
        this.prevTS = prevTS;
        this.nextTS = nextTS;
        aggregates = new Object[operators.length];
        for (int i = 0; i < aggregates.length; ++i) {
            aggregates[i] = operators[i].createEmpty(); // empty aggr
        }
    }


    @Override
    public String toString() {
        String ret = String.format("<summary-window: time range [%d:%d], count range [%d:%d], aggrs [", ts, te, cs, ce);
        boolean first = true;
        for (Object aggregate : aggregates) {
            if (first) {
                first = false;
            } else {
                ret += ", ";
            }
            ret += aggregate;
        }
        ret += "]>";
        return ret;
    }

}
