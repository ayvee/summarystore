package com.samsung.sra.DataStoreExperiments;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomWorkloadGenerator implements WorkloadGenerator<Long> {
    private final int A, L, Q;

    public RandomWorkloadGenerator(int A, int L, int Q) {
        this.A = A;
        this.L = L;
        this.Q = Q;

    }

    @Override
    public Workload<Long> generate(long T) {
        Random random = new Random(0);
        Workload<Long> ret = new Workload<>();
        List<AgeLengthClass> alClasses = AgeLengthSampler.getAgeLengthClasses(T, T, A, L);
        for (AgeLengthClass alClass : alClasses) {
            List<Workload.Query<Long>> thisClassQueries = new ArrayList<>();
            for (int q = 0; q < Q; ++q) {
                Pair<Long> al = alClass.sample(random);
                long age = al.first(), length = al.second();
                long l = T - length + 1 - age, r = T - age;
                if (0 <= l && r < T) {
                    Workload.Query<Long> query = new Workload.Query<>(l, r, 0, null, 0L);
                    thisClassQueries.add(query);
                }
            }
            ret.put(alClass.toString(), thisClassQueries);
        }
        return ret;
     }
}
