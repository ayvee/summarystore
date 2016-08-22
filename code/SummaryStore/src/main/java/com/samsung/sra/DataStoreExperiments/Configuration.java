package com.samsung.sra.DataStoreExperiments;

import com.moandjiezana.toml.Toml;
import com.samsung.sra.DataStore.*;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

/**
 * Configuration backed by a Toml file. See example.toml for a sample config.
 *
 * Raises IllegalArgumentException on some parse errors but is generally optimistic and expects the file is a legal
 * config.
 */
public class Configuration {
    private final Toml toml;

    public Configuration(File file) {
        if (!file.isFile()) throw new IllegalArgumentException("invalid or non-existent config file " + file);
        toml = new Toml().read(file);
    }

    /** Get working directory, where all files will be input/output */
    public String getDirectory() {
        return toml.getString("directory");
    }

    /** Optional prefix to add onto every input file */
    public String getPrefix() {
        return toml.getString("prefix", "");
    }

    /** Size of SummaryStore bucket cache */
    public long getBucketCacheSize() {
        return toml.getLong("bucket-cache-size", 0L);
    }

    public int getIngestBufferSize() {
        return toml.getLong("ingest-buffer-size", 0L).intValue();
    }

    /** Get prefix of all SummaryStore output directories. (NOTE: need to append decay function name to get the full
     * prefix that the SummaryStore constructor takes). {@link #getHash} explains why we use a hash here
     */
    public String getStorePrefix() {
        return String.format("%s/%sS%s", getDirectory(), getPrefix(), getHash(toml.getTable("data")));
    }

    public String getWorkloadFile() {
        return String.format("%s/%sS%s.W%s.workload",
                getDirectory(), getPrefix(), getHash(toml.getTable("data")), getHash(toml.getTable("workload")));
    }

    public String getProfileFile() {
        return String.format("%s/%sS%s.W%s.D%s.profile", getDirectory(), getPrefix(),
                getHash(toml.getTable("data")), getHash(toml.getTable("workload")), getHash(toml.getList("decay-functions")));
    }

    /** Data/queries will span the time range [0, T] */
    public long getT() {
        return toml.getLong("data.T");
    }

    public StreamGenerator getStreamGenerator() {
        Toml conf = toml.getTable("data");
        try {
            Constructor<?> ctor = Class.forName(String.format(
                    "com.samsung.sra.DataStoreExperiments.%sStreamGenerator", conf.getString("stream-generator")))
                    .getDeclaredConstructor(Toml.class);
            return (StreamGenerator) ctor.newInstance(conf);
        } catch (NoSuchMethodException | ClassNotFoundException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw new IllegalArgumentException("error trying to create StreamGenerator", e);
        }
    }

    /**
     * Compute a deterministic hash of some portion of the toml tree.
     * Use case: suppose we run several experiments trying various workloads against the same dataset. We want to
     * recognize that we can use the same generated SummaryStores for all these experiments. To do this we will name
     * SummaryStores using a hash of the dataset config portion of the toml.
     */
    public static String getHash(Object node) {
        HashCodeBuilder builder = new HashCodeBuilder();
        buildHash(node, builder);
        return Long.toString((long)builder.toHashCode() - (long)Integer.MIN_VALUE);
    }

    private static void buildHash(Object node, HashCodeBuilder builder) {
        if (node instanceof List) {
            for (Object entry: (List)node) { // Array. Hash entries in sequence
                buildHash(entry, builder);
            }
        } else if (node instanceof Toml || node instanceof Map) { // Table. Hash entries in key-sorted order
            Map<String, Object> map = node instanceof Toml ?
                    ((Toml) node).toMap() :
                    (Map<String, Object>)node;
            map.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> {
                        builder.append(e.getKey());
                        buildHash(e.getValue(), builder);
                    });
        } else { // a primitive
            assert node instanceof Number || node instanceof String || node instanceof Character
                    : "unknown node class " + node.getClass();
            builder.append(node); // TODO: verify that HashCodeBuilder handles Object-casted primitives sensibly
        }
    }

    /** Return list of all decay function names. Use parseDecayFunction to convert name to Windowing */
    public List<String> getDecayFunctions() {
        return toml.getList("decay-functions");
    }

    /** Convert decay function name to Windowing */
    public Windowing parseDecayFunction(String decayName) {
        if (decayName == null) {
            throw new IllegalArgumentException("expect non-null decay spec");
        } else if (decayName.startsWith("exponential")) {
            return new GenericWindowing(new ExponentialWindowLengths(Double.parseDouble(decayName.substring("exponential".length()))));
        } else if (decayName.startsWith("rationalPower")) {
            String[] pq = decayName.substring("rationalPower".length()).split(",");
            if (pq.length != 2 && pq.length != 4) throw new IllegalArgumentException("malformed rationalPower decay spec " + decayName);
            int P, Q, R, S;
            P = Integer.parseInt(pq[0]);
            Q = Integer.parseInt(pq[1]);
            if (pq.length == 4) {
                R = Integer.parseInt(pq[2]);
                S = Integer.parseInt(pq[3]);
            } else {
                R = 1;
                S = 1;
            }
            return new RationalPowerWindowing(P, Q, R, S);
        } else {
            throw new IllegalArgumentException("unrecognized decay function " + decayName);
        }
    }

    public WindowOperator[] getOperators() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        List<String> operatorNames = toml.getList("data.operators");
        WindowOperator[] operators = new WindowOperator[operatorNames.size()];
        for (int i = 0; i < operators.length; ++i) {
            String opname = operatorNames.get(i);
            operators[i] = (WindowOperator)Class.forName("com.samsung.sra.DataStore.Aggregates." + opname).newInstance();
        }
        return operators;
    }

    public WorkloadGenerator<Long> getWorkloadGenerator() {
        Toml conf = toml.getTable("workload");
        try {
            Constructor<?> ctor = Class.forName(String.format(
                    "com.samsung.sra.DataStoreExperiments.%sWorkloadGenerator", conf.getString("workload-generator")))
                    .getDeclaredConstructor(Toml.class);
            return (WorkloadGenerator<Long>) ctor.newInstance(conf);
        } catch (NoSuchMethodException | ClassNotFoundException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw new IllegalArgumentException("error trying to create WorkloadGenerator", e);
        }
    }

    public static Distribution<Long> parseDistribution(Toml conf) {
        try {
            Constructor<?> ctor = Class.forName(String.format(
                    "com.samsung.sra.DataStoreExperiments.%sDistribution", conf.getString("distribution")))
                    .getDeclaredConstructor(Toml.class);
            return (Distribution<Long>) ctor.newInstance(conf);
        } catch (NoSuchMethodException | ClassNotFoundException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw new IllegalArgumentException("error trying to create WorkloadGenerator", e);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration(
                new File("/Users/a.vulimiri/samsung/summarystore/code/SummaryStore/example.toml"));
        System.out.println(getHash(config.toml.getTable("data")));
    }
}
