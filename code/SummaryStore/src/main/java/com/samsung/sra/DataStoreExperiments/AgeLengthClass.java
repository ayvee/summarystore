package com.samsung.sra.DataStoreExperiments;

import org.apache.commons.math3.util.Pair;

import java.io.Serializable;
import java.util.Random;

public class AgeLengthClass implements Serializable {
    public static class Bin implements Serializable {
        public final String name;
        private final long start, end, multiplier;

        /**
         * The contents of this bin are the values
         *     start * multiplier, (start + 1) * multiplier, ..., end * multiplier
         */
        public Bin(String name, long start, long end, long multiplier) {
            this.name = name;
            this.start = start;
            this.end = end;
            this.multiplier = multiplier;
        }

        public Bin(Bin that) {
            this.name = that.name;
            this.start = that.start;
            this.end = that.end;
            this.multiplier = that.multiplier;
        }

        public long sample(Random rand) {
            return multiplier * (start + (Math.abs(rand.nextLong()) % (end - start + 1)));
        }

        public long getStart() {
            return start * multiplier;
        }

        @Override
        public String toString() {
            return name; //+ " [" + start + ", " + end + "] * " + multiplier;
        }
    }

    private final Bin ageBin, lengthBin;
    private final Long maxAge;

    public AgeLengthClass(Bin ageBin, Bin lengthBin) {
        this(ageBin, lengthBin, null);
    }

    public AgeLengthClass(Bin ageBin, Bin lengthBin, Long maxAge) {
        assert ageBin != null && lengthBin != null;
        this.ageBin = ageBin;
        this.lengthBin = lengthBin;
        this.maxAge = maxAge;
    }

    /** Return random age, random length */
    public Pair<Long, Long> sample(Random random) {
        // TODO: verify rejection sampling (1) is unbiased; (2) does not stall in sparse bins
        while (true) {
            long age = ageBin.sample(random);
            long length = lengthBin.sample(random);
            if (maxAge == null || age + length - 1 <= maxAge) {
                return new Pair<>(age, length);
            }
        }
    }

    @Override
    public String toString() {
        return "<" +
                "age " + ageBin + ", " +
                "length " + lengthBin +
                (maxAge != null ? (", maxAge = " + maxAge) : "") +
                ">";
    }
}
