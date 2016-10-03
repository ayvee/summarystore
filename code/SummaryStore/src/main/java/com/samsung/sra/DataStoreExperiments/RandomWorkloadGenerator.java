package com.samsung.sra.DataStoreExperiments;

import com.moandjiezana.toml.Toml;
import com.samsung.sra.DataStoreExperiments.Workload.Query;
import org.apache.commons.math3.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomWorkloadGenerator implements WorkloadGenerator {
    private final int A, L, Q;
    private final int operatorIndex;
    private final Query.Type operatorType;

    public RandomWorkloadGenerator(Toml params) {
        this.A = params.getLong("A").intValue();
        this.L = params.getLong("L").intValue();
        this.Q = params.getLong("Q").intValue();
        this.operatorIndex = params.getLong("operator.index").intValue();
        this.operatorType = Query.Type.valueOf(params.getString("operator.type").toUpperCase());
    }

    @Override
    public Workload generate(long T0, long T1) {
        Random random = new Random(0);
        Workload ret = new Workload();
        // Age/length classes will sample query ranges from [0, T1-T0]. We will add T0 below to compensate
        List<AgeLengthClass> alClasses = LogarithmicAgeLengths.getAgeLengthClasses(T1-T0+1, T1-T0+1, A, L);
        for (AgeLengthClass alClass : alClasses) {
            List<Query> thisClassQueries = new ArrayList<>();
            for (int q = 0; q < Q; ++q) {
                Pair<Long, Long> al = alClass.sample(random);
                long age = al.getFirst(), length = al.getSecond();
                long r = T1 - age, l = r - length + 1;
                if (T0 <= l && r <= T1) {
                    Query query = new Query(operatorType, l + T0, r + T0, operatorIndex, null, 0L);
                    thisClassQueries.add(query);
                }
            }
            ret.put(alClass.toString(), thisClassQueries);
        }
        return ret;
     }
}
