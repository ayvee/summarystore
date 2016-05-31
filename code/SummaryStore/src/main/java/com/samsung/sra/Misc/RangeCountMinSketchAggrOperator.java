package com.samsung.sra.Misc;

import com.samsung.sra.DataStore.WindowAggrOperator;
import com.samsung.sra.DataStore.WindowAggrOperatorTypes;

/**
 * Created by n.agrawal1 on 5/26/16.
 */
public class RangeCountMinSketchAggrOperator  {

    public RangeCountMinSketchAggrOperator(int opType) {
        //super(opType);
        //System.out.println("Currently registered operators: " + WindowAggrOperator.returnAggrOperatorTypes().size());

    }

    public RangeCountMinSketchAggrOperator merge (RangeCountMinSketchAggrOperator... rangeCountMinSketchAggrOperators) {

        RangeCountMinSketchAggrOperator rangeCountMinSketchAggrOperator =
                new RangeCountMinSketchAggrOperator(WindowAggrOperatorTypes.AggrOpTypes.RCMS.ordinal());
        for (RangeCountMinSketchAggrOperator o:rangeCountMinSketchAggrOperators) {
            System.out.print(o.toString());
        }

        return rangeCountMinSketchAggrOperator;

    }
}
