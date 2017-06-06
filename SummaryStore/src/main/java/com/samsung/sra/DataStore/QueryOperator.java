package com.samsung.sra.DataStore;

import com.samsung.sra.protocol.Common;
import com.samsung.sra.protocol.Common.Operator;
import com.samsung.sra.protocol.Common.OpType;

/**
 * Created by n.agrawal1 on 6/6/17.
 */
public class QueryOperator {

    private OpType type;
    private Object[] params = null;

    QueryOperator() {}

    QueryOperator(OpType type, Object... params) {
        this.type = type;
        if(params!=null && params.length>0) {
            this.params = new Object[params.length];
            System.arraycopy(params, 0, this.params, 0, params.length);
        }
    }

    public OpType getOpType() {
        return type;
    }

    public Object[] getParams() {
        return params;
    }
}
