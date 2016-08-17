package com.samsung.sra.DataStoreExperiments;

import com.moandjiezana.toml.Toml;
import com.samsung.sra.DataStore.*;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.io.File;
import java.io.IOException;
import java.util.List;

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

    /** Get prefix of all SummaryStore output directories. (NOTE: need to append
     * decay function name to get the full prefix that SummaryStore constructor takes)
     */
    public String getStorePrefix() {
        return String.format("%s/%sT%d.S%s", getDirectory(), getPrefix(), getT(), getStreamGeneratorHash());
    }

    /** Return storeprefix minus directory path */
    public String getStorePrefixBasename() {
        return String.format("%sT%d.S%s", getPrefix(), getT(), getStreamGeneratorHash());
    }

    public String getWorkloadFile() {
        return String.format("%s/%sT%d.S%s.W%s.workload",
                getDirectory(), getPrefix(), getT(), getStreamGeneratorHash(), getWorkloadGeneratorHash());
    }

    public String getProfileFile() {
        return String.format("%s/%sT%d.S%s.W%s.profile",
                getDirectory(), getPrefix(), getT(), getStreamGeneratorHash(), getWorkloadGeneratorHash());
    }

    /** Data/queries will span the time range [0, T] */
    public long getT() {
        return toml.getLong("data.T");
    }

    public StreamGenerator getStreamGenerator() throws IOException {
        Toml conf = toml.getTable("data");
        switch (conf.getString("class").toLowerCase()) {
            case "random": {
                Distribution<Long> I, V;
                long R;
                switch (conf.getString("I.class").toLowerCase()) {
                    case "exponential":
                        I = new ExponentialDistribution(conf.getDouble("I.lambda"));
                        break;
                    case "fixed":
                        I = new FixedDistribution(conf.getLong("I.value"));
                        break;
                    default:
                        throw new IllegalArgumentException("invalid or missing interarrival class");
                }
                switch (conf.getString("V.class").toLowerCase()) {
                    case "uniform":
                        V = new UniformDistribution(conf.getLong("V.min"), conf.getLong("V.max"));
                        break;
                    default:
                        throw new IllegalArgumentException("invalid or missing value class");
                }
                R = conf.getLong("R", 0L);
                return new RandomStreamGenerator(I, V, R);
            }
            case "replay":
                return new ReplayStreamGenerator(conf.getString("file"));
            default:
                throw new IllegalArgumentException("invalid or missing stream generator class");
        }
    }

    public String getStreamGeneratorHash() {
        Toml conf = toml.getTable("data");
        // TODO: consider using say MD5 instead?
        HashCodeBuilder builder = new HashCodeBuilder();
        switch (conf.getString("class").toLowerCase()) {
            case "random":
                builder.append("random");
                switch (conf.getString("I.class").toLowerCase()) {
                    case "exponential":
                        builder.append("exponential");
                        builder.append(conf.getDouble("I.lambda"));
                        break;
                    case "fixed":
                        builder.append("fixed");
                        builder.append(conf.getLong("I.value"));
                        break;
                    default:
                        throw new IllegalArgumentException("invalid or missing interarrival class");
                }
                switch (conf.getString("V.class").toLowerCase()) {
                    case "uniform":
                        builder.append("uniform");
                        builder.append(conf.getLong("V.min"));
                        builder.append(conf.getLong("V.max"));
                        break;
                    default:
                        throw new IllegalArgumentException("invalid or missing value class");
                }
                builder.append(conf.getLong("R", 0L));
                break;
            case "replay":
                builder.append("replay");
                builder.append(conf.getString("file"));
                break;
            default:
                throw new IllegalArgumentException("invalid or missing stream generator class");
        }
        return Long.toString((long)builder.toHashCode() - (long)Integer.MIN_VALUE);
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
            if (pq.length != 2) throw new IllegalArgumentException("malformed rationalPower decay spec " + decayName);
            return new RationalPowerWindowing(Integer.parseInt(pq[0]), Integer.parseInt(pq[1]));
        } else {
            throw new IllegalArgumentException("unrecognized decay function " + decayName);
        }
    }

    public WindowOperator[] getOperators() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        List<String> operatorNames = toml.getList("dataset.operators");
        WindowOperator[] operators = new WindowOperator[operatorNames.size()];
        for (int i = 0; i < operators.length; ++i) {
            String opname = operatorNames.get(i);
            operators[i] = (WindowOperator)Class.forName("com.samsung.sra.DataStore.Aggregates." + opname).newInstance();
        }
        return operators;
    }

    public WorkloadGenerator<Long> getWorkloadGenerator() {
        Toml conf = toml.getTable("workload");
        switch (conf.getString("class").toLowerCase()) {
            case "random":
                return new RandomWorkloadGenerator(
                        conf.getLong("A").intValue(), conf.getLong("L").intValue(), conf.getLong("Q").intValue()
                );
            default:
                throw new IllegalArgumentException("invalid or missing workload generator class");
        }
    }

    public String getWorkloadGeneratorHash() {
        Toml conf = toml.getTable("workload");
        // TODO: consider using say MD5 instead?
        HashCodeBuilder builder = new HashCodeBuilder();
        switch (conf.getString("class").toLowerCase()) {
            case "random":
                builder.append(conf.getLong("A"));
                builder.append(conf.getLong("L"));
                builder.append(conf.getLong("Q"));
                break;
            default:
                throw new IllegalArgumentException("invalid or missing workload generator class");
        }
        return Long.toString((long)builder.toHashCode() - (long)Integer.MIN_VALUE);
    }

    public static void main(String[] args) throws Exception {
        System.out.println(new Configuration(
                new File("/Users/a.vulimiri/samsung/summarystore/code/SummaryStore/example.toml"))
                .getWorkloadFile());
    }
}
