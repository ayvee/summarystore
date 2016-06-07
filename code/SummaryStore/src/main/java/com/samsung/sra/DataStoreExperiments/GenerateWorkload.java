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

public class GenerateWorkload {
    private static Logger logger = LoggerFactory.getLogger(GenerateWorkload.class);

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

    public List<Query<Long>> generate(StreamGenerator streamGenerator, long N, int Q) {
        LongRange[] intervals = new LongRange[Q];
        long[] trueCounts = new long[Q];

        Random random = new Random(0);
        for (int q = 0; q < Q; ++q) {
            long a = Math.abs(random.nextLong()) % N, b = Math.abs(random.nextLong()) % N;
            long l = Math.min(a, b), r = Math.max(a, b);
            intervals[q] = new LongRange(l + ":" + r, l, true, r, true);
            trueCounts[q] = 0;
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

        List<Query<Long>> res = new ArrayList<>();
        for (int q = 0; q < Q; ++q) {
            res.add(new Query<>(intervals[q].min, intervals[q].max, QueryType.COUNT, null, trueCounts[q]));
        }
        return res;
    }

    public static void main(String[] args) {
        StreamGenerator streamGenerator = new StreamGenerator(new FixedInterarrival(1), new UniformValues(0, 100), 0);
        GenerateWorkload generateWorkload = new GenerateWorkload();
        List<Query<Long>> workload = generateWorkload.generate(streamGenerator, 10_000_000, 1000);
        for (Query<Long> q: workload) {
            assert q.r - q.l + 1 == q.trueAnswer;
            //System.out.println(q);
        }
    }
}
