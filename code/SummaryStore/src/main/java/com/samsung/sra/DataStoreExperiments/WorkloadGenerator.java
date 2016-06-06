package com.samsung.sra.DataStoreExperiments;

import com.changingbits.Builder;
import com.changingbits.LongRange;
import com.changingbits.LongRangeMultiSet;
import com.samsung.sra.DataStore.QueryType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class WorkloadGenerator {
    private static Logger logger = LoggerFactory.getLogger(WorkloadGenerator.class);

    public static class Query<R> {
        long l, r;
        QueryType type;
        Object[] params;
        R trueAnswer;

        public Query(long l, long r, QueryType type, Object[] params, R trueAnswer) {
            this.l = l;
            this.r = r;
            this.type = type;
            this.params = params;
            this.trueAnswer = trueAnswer;
        }

        @Override
        public String toString() {
            return type + "[" + l + ", " + r + "] = " + trueAnswer;
        }
    }

    public List<Query> generate(StreamGenerator streamGenerator, long N, int Q) {
        LongRange[] intervals = new LongRange[Q];
        long[] trueCounts = new long[Q];

        /*intervals = new LongRange[4];
        intervals[0] = new LongRange("0:1", 0, true, 1, true);
        intervals[1] = new LongRange("1:2", 1, true, 2, true);
        intervals[2] = new LongRange("3:6", 3, true, 6, true);
        intervals[3] = new LongRange("0:5", 0, true, 5, true);
        LongRangeMultiSet set = new Builder(intervals).getMultiSet(false, true);
        int[] results = new int[4];
        for (int t = 0; t < 10; ++t) {
            System.out.print(t + ":");
            int resCount = set.lookup(t, results);
            for (int i = 0; i < resCount; ++i) {
                System.out.print(" " + intervals[results[i]]);
            }
            System.out.println();
        }
        System.exit(0);*/

        Random random = new Random(0);
        for (int q = 0; q < Q; ++q) {
            long a = Math.abs(random.nextLong()) % N, b = Math.abs(random.nextLong()) % N;
            long l = Math.min(a, b), r = Math.max(a, b);
            intervals[q] = new LongRange(l + ":" + r, l, true, r, true);
            trueCounts[q] = 0;
            //Query<Long> query = new Query<>(l, r, QueryType.COUNT, null, 0L);
            //ist.put(new Interval1D(l, r), query);
        }

        Builder builder = new Builder(intervals, 0, N);
        LongRangeMultiSet lrms = builder.getMultiSet(false, true);
        int[] matchedIndexes = new int[Q];

        streamGenerator.generate(N, (t, v) -> {
            if (t % 1_000_000 == 0) {
                logger.info("t = {}", t);
            }
            int matchCount = lrms.lookup(t, matchedIndexes);
            for (int i = 0; i < matchCount; ++i) {
                ++trueCounts[matchedIndexes[i]];
            }
        });

        List<Query> res = new ArrayList<>();
        for (int q = 0; q < Q; ++q) {
            res.add(new Query<>(intervals[q].min, intervals[q].max, QueryType.COUNT, null, trueCounts[q]));
        }
        return res;
    }

    public static void main(String[] args) {
        StreamGenerator streamGenerator = new StreamGenerator(new FixedInterarrival(1), new UniformValues(0, 100), 0);
        WorkloadGenerator workloadGenerator = new WorkloadGenerator();
        List<Query> workload = workloadGenerator.generate(streamGenerator, 100_000_000, 1000);
        /*for (Query q: workload) {
            System.out.println(q);
        }*/
    }
}
