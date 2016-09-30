package com.samsung.sra.DataStoreExperiments;

import org.apache.commons.math3.util.Pair;

import java.io.Serializable;
import java.util.Random;

public class AgeLengthClass implements Serializable {
    public static class Range<T> implements Serializable {
        public final T min, max;

        Range(T min, T max) {
            assert min != null && max != null;
            this.min = min;
            this.max = max;
        }

        @Override
        public String toString() {
            return "[" + min + ", " + max + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Range<?> range = (Range<?>) o;

            return min.equals(range.min) && max.equals(range.max);
        }

        @Override
        public int hashCode() {
            int result = min.hashCode();
            result = 31 * result + max.hashCode();
            return result;
        }
    }

    private final Range<Long> ageRange, lengthRange;

    public AgeLengthClass(Range<Long> ageRange,
                          Range<Long> lengthRange) {
        assert ageRange != null && lengthRange != null;
        this.ageRange = ageRange;
        this.lengthRange = lengthRange;
    }

    public Pair<Long, Long> sample(Random random) {
        double aRand = random.nextDouble(), lRand = random.nextDouble();
        return new Pair<>(
                (long) (ageRange.min + aRand * (ageRange.max - ageRange.min)),
                (long) (lengthRange.min + lRand * (lengthRange.max - lengthRange.min)));
    }

    @Override
    public String toString() {
        return "<ages " + ageRange + ", lengths " + lengthRange + ">";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AgeLengthClass that = (AgeLengthClass) o;

        return ageRange.equals(that.ageRange) && lengthRange.equals(that.lengthRange);
    }

    @Override
    public int hashCode() {
        int result = ageRange.hashCode();
        result = 31 * result + lengthRange.hashCode();
        return result;
    }
}
