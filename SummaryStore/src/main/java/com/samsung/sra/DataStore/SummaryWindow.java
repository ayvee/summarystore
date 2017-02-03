package com.samsung.sra.DataStore;

import java.io.Serializable;

/**
 * Window holding a set of summary data structures. SummaryStore stream = list of contiguous SummaryWindows
 *
 * Dumb structs. All the code creating, manipulating, and querying windows is in StreamManager */
public class SummaryWindow implements Serializable {
    // metadata
    /* We use longs for window IDs, timestamps, and count markers. Valid values should be
       non-negative (all three are 0-indexed); use "-1" to indicate null values. */
    // TODO: weaken access modifiers
    public long prevSWID, thisSWID, nextSWID; // SWID = Summary Window ID
    public long tStart, tEnd;
    public long cStart, cEnd;

    // data
    public Object[] aggregates;

    SummaryWindow() {}

    SummaryWindow(WindowOperator[] operators,
                  long prevSWID, long thisSWID, long nextSWID,
                  long tStart, long tEnd, long cStart, long cEnd) {
        this.prevSWID = prevSWID;
        this.thisSWID = thisSWID;
        this.nextSWID = nextSWID;
        this.tStart = tStart;
        this.tEnd = tEnd;
        this.cStart = cStart;
        this.cEnd = cEnd;
        aggregates = new Object[operators.length];
        for (int i = 0; i < aggregates.length; ++i) {
            aggregates[i] = operators[i].createEmpty(); // empty aggr
        }
    }


    @Override
    public String toString() {
        String ret = "<summary-window " + thisSWID;
        ret += ", time range [" + tStart + ":" + tEnd + "]";
        ret += ", count range [" + cStart + ":" + cEnd + "]";
        ret += ", prev, next ID [" + prevSWID + ":" + nextSWID +"]";
        ret += ", aggrs [ Count = " + aggregates.length + " : ";
        /*for (Object aggregate : aggregates) {
            ret += aggregate.getClass().toString() + " : ";
            ret += aggregate.toString() + " : ";
        }*/
        ret += "]";
        //ret += ", count = " + (cEnd - cStart + 1);
        ret += ">";
        return ret;
    }

}
