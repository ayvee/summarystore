package com.samsung.sra.experiments;

import com.moandjiezana.toml.Toml;
import com.samsung.sra.experiments.Workload.Query;
import org.apache.commons.math3.util.Pair;

import java.util.*;

/** Generate a workload of random count/sum/etc queries with calendar age/lengths. */
public class SampledCalendarWorkloadGenerator implements WorkloadGenerator {
    private final List<OperatorInfo> operators = new ArrayList<>();
    private final long ticksPerS;
    private final boolean rescaleToYear;
    private final String smallestBin;
    private final Long queriesPerClass;

    private static class OperatorInfo {
        final int index;
        final Query.Type type;
        final Distribution<Long> valueParamDistr;

        OperatorInfo(Toml conf) {
            index = conf.getLong("index").intValue();
            type = Query.Type.valueOf(conf.getString("type").toUpperCase());
            if (type == Query.Type.BF || type == Query.Type.CMS) {
                Toml cmsParamSpec = conf.getTable("param");
                assert cmsParamSpec != null;
                valueParamDistr = Configuration.parseDistribution(cmsParamSpec);
            } else {
                valueParamDistr = null;
            }
        }

        Query getQuery(long l, long r, SplittableRandom rand) {
            Object[] params = (type == Query.Type.BF || type == Query.Type.CMS)
                    ? new Object[]{valueParamDistr.next(rand)}
                    : null;
            return new Query(type, l, r, index, params);
        }
    }

    public SampledCalendarWorkloadGenerator(Toml conf) {
        ticksPerS = conf.getLong("ticks-per-second", 1L);
        for (Toml opConf: conf.getTables("operators")) {
            operators.add(new OperatorInfo(opConf));
        }
        rescaleToYear = conf.getBoolean("rescale-to-year", false);
        smallestBin = conf.getString("smallest-bin", "seconds");
        queriesPerClass = conf.getLong("queries-per-class");
    }

    @Override
    public Workload generate(long T0, long T1) {
        SplittableRandom rand = new SplittableRandom(0);
        Workload workload = new Workload();
        // Age/length classes will sample query ranges from [0s, (T1-T0) in seconds]. We will rescale below to correct
        List<AgeLengthClass> alClasses = CalendarAgeLengths.getClasses(
                rescaleToYear ? 365 * 86400L : (T1 - T0) / ticksPerS,
                smallestBin);
        // FIXME? will not work properly if user specifies more than one operator of the same type (e.g. two CMS operators)
        for (OperatorInfo operator: operators) {
            for (AgeLengthClass alCls : alClasses) {
                String groupName = String.format("%s\t%s", operator.type, alCls.toString());
                List<Query> groupQueries = new ArrayList<>();
                workload.put(groupName, groupQueries);
                Collection<Pair<Long, Long>> classQueries = alCls.getAllAgeLengths();
                if (queriesPerClass != null && queriesPerClass < classQueries.size()) {
                    List<Pair<Long, Long>> shuffle = new ArrayList<>(classQueries);
                    Collections.shuffle(shuffle);
                    classQueries = shuffle.subList(0, queriesPerClass.intValue());
                }
                for (Pair<Long, Long> ageLength: classQueries) {
                    long age, length, l, r;
                    if (rescaleToYear) {
                        age = ageLength.getFirst();
                        length = ageLength.getSecond();
                        r = 365 * 86400L - age;
                        l = r - length + 1;
                        // [l, r] is now a subrange of [0, 365 * 86400L]; rescale to [T0, T1]
                        l = T0 + Math.round((T1 - T0) * (l / 365d / 86400d));
                        r = T0 + Math.round((T1 - T0) * (r / 365d / 86400d));
                    } else {
                        age = ageLength.getFirst() * ticksPerS;
                        length = ageLength.getSecond() * ticksPerS;
                        r = T1 - age;
                        l = r - length + ticksPerS;
                    }
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
