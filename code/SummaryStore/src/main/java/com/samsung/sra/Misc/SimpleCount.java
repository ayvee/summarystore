package com.samsung.sra.Misc;

import com.samsung.sra.DataStore.ValueError;
import com.samsung.sra.DataStore.WindowAggrOperator;

/**
 * Created by n.agrawal1 on 5/30/16.
 */
public class SimpleCount extends WindowAggrOperator<SimpleCount, Integer>{

    private int count;

    public SimpleCount merge(SimpleCount... simpleCounts) {

        SimpleCount mergedsimpleCount = new SimpleCount();
        for (SimpleCount s:simpleCounts) {
            mergedsimpleCount.count += s.count;
        }

        return mergedsimpleCount;
    }


    // return value for entire bucket
    public Integer read() {
        return this.count;
    }

    // return partial bucket - simple bound or computed based on AggrOperator
    public ValueError<Integer> readRange(long tStart, long tEnd) {
        ValueError<Integer> ve = new ValueError<>();
        ve.value = this.count;
        ve.error = getError(tStart,tEnd);

        return ve;
    }

    // define at least one error functon for tStart, tEnd
    public Integer getError(long tStart, long tEnd) {
        return 0;
    }
}
