package com.samsung.sra.DataStoreExperiments;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A "workload" is a binned list of queries, essentially {
 *      name of 1st query class -> list of all queries belonging to 1st query class,
 *      name of 2nd query class -> list of all queries belonging to 2nd query class,
 *      ...
 * }
 * @param <R>    query result type
 */
public class Workload<R> extends ConcurrentHashMap<String, List<Workload.Query<R>>> {
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