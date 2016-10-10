package com.samsung.sra.DataStoreExperiments;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A "workload" is a binned list of queries, essentially {
 *      name of 1st query group -> list of all queries belonging to 1st query group,
 *      name of 2nd query group -> list of all queries belonging to 2nd query group,
 *      ...
 * }
 */
public class Workload extends LinkedHashMap<String, List<Workload.Query>> {
    public static class Query implements Serializable {
        public enum Type {
            COUNT,
            SUM,
            CMS
        }
        Type queryType;
        long l, r;
        int operatorNum;
        Object[] params;
        AtomicLong trueAnswer = new AtomicLong(0L);

        public Query(Type queryType, long l, long r, int operatorNum, Object[] params) {
            this.queryType = queryType;
            this.l = l;
            this.r = r;
            this.operatorNum = operatorNum;
            this.params = params;
        }

        @Override
        public String toString() {
            return "trueAnswer[" + l + ", " + r + "] = " + trueAnswer;
        }
    }
}