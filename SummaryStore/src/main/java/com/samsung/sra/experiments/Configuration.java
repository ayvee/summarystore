package com.samsung.sra.experiments;

import com.moandjiezana.toml.Toml;
import com.samsung.sra.datastore.*;
import com.samsung.sra.datastore.aggregates.BloomFilterOperator;
import com.samsung.sra.datastore.aggregates.CMSOperator;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.File;
import java.io.IOException;
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

    public Toml getToml() {
        return toml;
    }

    /** Optional prefix to add onto every input file */
    public String getPrefix() {
        return toml.getString("prefix", "");
    }

    /** Size of SummaryStore window cache per stream (in readonly mode) */
    public long getWindowCacheSize() {
        return toml.getLong("window-cache-size", 0L);
    }

    /** Size of ingest buffer size per stream (in read/write mode) */
    public int getIngestBufferSize() {
        return toml.getLong("ingest-buffer-size", 0L).intValue();
    }

    /** Where all SummaryStore data will be stored */
    private String getDataDirectory() {
        return toml.getString("data-dir");
    }

    /** Where all experiment files will be input/output */
    private String getResultsDirectory() {
        return toml.getString("results-dir");
    }

    /** Get SummaryStore output directory. {@link #getHash} explains why we use a hash here */
    public String getStoreDirectory(String decayName) {
        long nstreams = getNStreams();
        String streamPrefix = nstreams > 1 ? "N" + nstreams + "." : "";
        return String.format("%s/%s%sS%s.O%s.D%s",
                getDataDirectory(), getPrefix(),streamPrefix,
                getHash(toml.getTable("data")), getHash(toml.getList("operators")), decayName);
    }

    public String getWorkloadFile() {
        return String.format("%s/%sS%s.W%s.workload",
                getResultsDirectory(), getPrefix(), getHash(toml.getTable("data")), getHash(toml.getTable("workload")));
    }

    public String getProfileFile(String confidenceLevel) {
        String prefix =  String.format("%s/%sS%s.W%s.O%s.D%s", getResultsDirectory(), getPrefix(),
                getHash(toml.getTable("data")), getHash(toml.getTable("workload")),
                getHash(toml.getList("operators")), getHash(toml.getList("decay-functions")));
        return prefix
                + (confidenceLevel != null ? ".C" + confidenceLevel : "")
                + ".profile";
    }

    /** Data/queries will span the time range [tstart, tend] */
    public long getTstart() {
        return toml.getLong("data.tstart");
    }

    /** Data/queries will span the time range [tstart, tend] */
    public long getTend() {
        return toml.getLong("data.tend");
    }

    private StreamGenerator cachedBaseStreamGenerator = null;

    // TODO: add synchronized. Not needed for all our uses as of 04/13/2017
    public StreamGenerator getBaseStreamGenerator() {
        Toml conf = toml.getTable("data");
        if (cachedBaseStreamGenerator != null) {
            return cachedBaseStreamGenerator.copy();
        } else {
            StreamGenerator gen = constructObjectViaReflection(
                    "com.samsung.sra.experiments." + conf.getString("stream-generator"),
                    conf);
            if (gen.isCopyable()) {
                cachedBaseStreamGenerator = gen;
            }
            return gen;
        }
    }

    public StreamGenerator getStreamGenerator() {
        Toml conf = toml.getTable("data");
        StreamGenerator baseStream = getBaseStreamGenerator();
        Toml taggerConf = conf.getTable("tag-landmarks");
        if (taggerConf != null) {
            try {
                return new LandmarkTagger(baseStream, taggerConf);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            return baseStream;
        }
    }

    /** How many enum answers to load in memory per batch when using PopulateWorkload */
    public long getEnumBatchSize() {
        long defaultVal = 1_000_000_000;
        Toml conf = toml.getTable("performance");
        return conf != null ? conf.getLong("enum-batch-size", defaultVal) : defaultVal;
    }


    /** FIXME */
    public RandomStreamIterator getStreamIterator(long streamID) {
        Toml conf = toml.getTable("data");
        Distribution<Long>
                interarrivals = Configuration.parseDistribution(conf.getTable("interarrivals")),
                values = Configuration.parseDistribution(conf.getTable("values"));
        RandomStreamIterator ris = new RandomStreamIterator(interarrivals, values, streamID);
        ris.setTimeRange(getTstart(), getTend());
        return ris;
    }

    public RandomStreamIterator getStreamIterator() {
        return getStreamIterator(0L);
    }

    public ParRandomStreamIterator getParStreamIterator(long streamID) {
        ParRandomStreamIterator pris = new ParRandomStreamIterator(streamID);
        pris.setTimeRange(getTstart(), getTend());
        return pris;
    }

    public int getNStreams() {
        Toml conf = toml.getTable("streams");
        return conf == null ? 1 : conf.getLong("nstreams", 1L).intValue();
    }

    public long getNStreamsPerShard() {
        Toml conf = toml.getTable("streams");
        return conf == null ? 1 : conf.getLong("nstreams-per-shard", 1L).intValue();
    }

    public int getNumIngestThreads() {
        Toml conf = toml.getTable("performance");
        return conf.getLong("num-ingest-threads", getNStreamsPerShard()).intValue();
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
            assert node instanceof Number || node instanceof String || node instanceof Character || node instanceof Boolean
                    : "unknown node class " + node.getClass();
            builder.append(node);
        }
    }

    private static <T> T constructObjectViaReflection(String className, Toml conf) {
        try {
            return (T) Class.forName(className).getConstructor(Toml.class).newInstance(conf);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("could not construct object of type " + className, e);
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

    public WindowOperator[] getOperators() {
        List<String> operatorNames = toml.getList("operators");
        WindowOperator[] operators = new WindowOperator[operatorNames.size()];
        for (int i = 0; i < operators.length; ++i) {
            String opname = operatorNames.get(i);
            if (opname.startsWith("CMSOperator")) {
                String[] params = opname.substring("CMSOperator".length()).split(",");
                int depth = Integer.parseInt(params[0]);
                int width = Integer.parseInt(params[1]);
                int seed = 0;
                operators[i] = new CMSOperator(depth, width, seed);
            } else if (opname.startsWith("BloomFilterOperator")) {
                String[] params = opname.substring("BloomFilterOperator".length()).split(",");
                int numHashes = Integer.parseInt(params[0]);
                int filterSize = Integer.parseInt(params[1]);
                operators[i] = new BloomFilterOperator(numHashes, filterSize);
            } else {
                try {
                    operators[i] = (WindowOperator) Class.forName("com.samsung.sra.datastore.aggregates." + opname).newInstance();
                } catch (ReflectiveOperationException e) {
                    throw new IllegalArgumentException("could not construct operator " + opname, e);
                }
            }
        }
        return operators;
    }

    public WorkloadGenerator getWorkloadGenerator() {
        Toml conf = toml.getTable("workload");
        return constructObjectViaReflection(
                "com.samsung.sra.experiments." + conf.getString("workload-generator"),
                conf);
    }

    /**
     * Compute true answers to queries in parallel when generating workload. WARNING: stream seeking adds a couple
     * of minutes of overhead, only worth enabling for large workloads.
     */
    public boolean isWorkloadParallelismEnabled() {
        Toml conf = toml.getTable("performance");
        return conf != null && conf.getBoolean("parallel-workload-gen", false);
    }

    /**
     * Drop kernel page/inode/dentries caches before testing each SummaryStore in RunComparison
     */
    public boolean dropKernelCaches() {
        Toml conf = toml.getTable("performance");
        return conf != null && conf.getBoolean("drop-caches", false);
    }

    public static Distribution<Long> parseDistribution(Toml conf) {
        return constructObjectViaReflection(
                "com.samsung.sra.experiments." + conf.getString("distribution"),
                conf);
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration(
                new File("/Users/a.vulimiri/samsung/summarystore/code/SummaryStore/example.toml"));
        System.out.println(getHash(config.toml.getTable("data")));
    }
}
