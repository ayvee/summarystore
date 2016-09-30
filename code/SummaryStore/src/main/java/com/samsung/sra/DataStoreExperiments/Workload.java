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
 */
public class Workload extends ConcurrentHashMap<String, List<Workload.Query>> {
    public static class Query implements Serializable {
        String queryType;
        long l, r;
        int operatorNum;
        Object[] params;
        long trueAnswer;

        public Query(String queryType, long l, long r, int operatorNum, Object[] params, long trueAnswer) {
            this.queryType = queryType;
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