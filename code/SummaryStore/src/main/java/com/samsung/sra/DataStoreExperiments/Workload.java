package com.samsung.sra.DataStoreExperiments;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Workload<T> extends ConcurrentHashMap<String, List<Workload.Query<T>>> {
    public static class Query<R> implements Serializable {
        long l, r;
        int operatorNum;
        Object[] params;
        R trueAnswer;

        public Query(long l, long r, int operatorNum, Object[] params, R trueAnswer) {
            this.l = l;
            this.r = r;
            this.operatorNum = operatorNum;
            this.params = params;
            this.trueAnswer = trueAnswer;
        }

        @Override
        public String toString() {
            return "trueAnswer[" + l + ", " + r + "] = " + trueAnswer;
        }
    }
}