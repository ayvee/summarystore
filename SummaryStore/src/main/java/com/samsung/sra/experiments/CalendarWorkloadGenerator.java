package com.samsung.sra.experiments;

import com.moandjiezana.toml.Toml;
import com.samsung.sra.experiments.Workload.Query;
import org.apache.commons.math3.util.Pair;

import java.util.*;

/** Generate a workload of random queries with calendar age/lengths. */
public class CalendarWorkloadGenerator implements WorkloadGenerator {
    private final List<OperatorInfo> operators = new ArrayList<>();
    private final long ticksPerS;

    private static class OperatorInfo {
        final int index;
        final Query.Type type;
        final Distribution<Long> valueParamDistr;

        OperatorInfo(Toml conf) {
            index = conf.getLong("index").intValue();
            type = Query.Type.valueOf(conf.getString("type").toUpperCase());
            Toml paramSpec = conf.getTable("param");
            switch (type) {
                case BF:
                case CMS:
                    valueParamDistr = Configuration.parseDistribution(paramSpec);
                    break;
                case MAX_THRESH:
                    valueParamDistr = new FixedDistribution(paramSpec.getLong("threshold"));
                    break;
                default:
                    valueParamDistr = null;
                    break;
            }
        }

        Query getQuery(long l, long r, Random rand) {
            Object[] params = valueParamDistr != null
                    ? new Object[]{valueParamDistr.next(rand)}
                    : null;
            return new Query(type, l, r, index, params);
        }
    }

    public CalendarWorkloadGenerator(Toml conf) {
        ticksPerS = conf.getLong("ticks-per-second", 1L);
        for (Toml opConf: conf.getTables("operators")) {
            operators.add(new OperatorInfo(opConf));
        }
    }

    @Override
    public Workload generate(long T0, long T1) {
        Random rand = new Random(0);
        Workload workload = new Workload();
        // Age/length classes will sample query ranges from [0s, (T1-T0) in seconds]. We will rescale below to correct
        List<AgeLengthClass> alClasses = CalendarAgeLengths.getClasses((T1 - T0) / ticksPerS);
        for (OperatorInfo operator: operators) {
            for (AgeLengthClass alCls : alClasses) {
                String groupName = String.format("%s\t%s", operator.type, alCls.toString());
                List<Query> groupQueries = new ArrayList<>();
                workload.put(groupName, groupQueries);
                for (Pair<Long, Long> ageLength: alCls.getAllAgeLengths()) {
                    long age = ageLength.getFirst() * ticksPerS, length = ageLength.getSecond() * ticksPerS;
                    long r = T1 - age, l = r - length + ticksPerS;
                    assert T0 <= l && l <= r && r <= T1 :
                            String.format("[T0, T1] = [%s, %s], age = %s, length = %s, [l, r] = [%s, %s]",
                                    T0, T1, age, length, l, r);
                    groupQueries.add(operator.getQuery(l, r, rand));
                }
            }
        }
        return workload;
    }
}
