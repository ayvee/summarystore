package com.samsung.sra.Misc;

import com.samsung.sra.DataStore.ValueError;
import com.samsung.sra.DataStore.WindowAggrOperator;

/**
 * Created by n.agrawal1 on 5/30/16.
 */
public class SimpleSum extends WindowAggrOperator<SimpleSum, Integer>{

    private int sum;

    public SimpleSum merge(SimpleSum... simpleSums) {

        SimpleSum mergedsimpleSum = new SimpleSum();
        for (SimpleSum s:simpleSums) {
            mergedsimpleSum.sum += s.sum;
        }

        return mergedsimpleSum;
    }


    // return value for entire bucket
    public Integer read() {
        return this.sum;
    }

    // return partial bucket - simple bound or computed based on AggrOperator
    public ValueError<Integer> readRange(long tStart, long tEnd) {
        ValueError<Integer> ve = new ValueError<>();
        ve.value = this.sum;
        ve.error = getError(tStart,tEnd);

        return ve;
    }

    // define at least one error functon for tStart, tEnd
    public Integer getError(long tStart, long tEnd) {
        return 0;
    }
}
