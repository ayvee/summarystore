package com.samsung.sra.DataStore;

/**
 * Created by n.agrawal1 on 5/26/16.
 */

import com.samsung.sra.DataStore.Bucket;

import java.util.Vector;

public abstract class WindowAggrOperator<T extends WindowAggrOperator, Y> {

    // base class can count number of OpTypes currently implemented by derived classes
    private static Vector opTypes;

    // must define a merge function for two or more buckets
    abstract public T merge(T... objects);

    // return value for entire bucket
    abstract public Y read();

    // return partial bucket - simple bound or computed based on AggrOperator
    abstract public ValueError<Y> readRange(long tStart, long tEnd);

    // define at least one error functon for tStart, tEnd
    abstract public Y getError(long tStart, long tEnd);

    /*
    public WindowAggrOperator() {
         opTypes = new Vector();
    }

    // to be used by derived classes
    public WindowAggrOperator(int opType) {
        if(! opTypes.contains(opType))
             opTypes.add(opType);
    }

    public static Vector<Integer> returnAggrOperatorTypes() {
        return  opTypes;
    }

    */

}
