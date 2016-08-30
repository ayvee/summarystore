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

            if (!min.equals(range.min)) return false;
            return max.equals(range.max);

        }

        @Override
        public int hashCode() {
            int result = min.hashCode();
            result = 31 * result + max.hashCode();
            return result;
        }
    }

    public final int ageClassNum, lengthClassNum;
    public final Range<Long> ageRange, lengthRange;

    public AgeLengthClass(int ageClassNum, Range<Long> ageRange,
                          int lengthClassNum, Range<Long> lengthRange) {
        this.ageClassNum = ageClassNum;
        this.lengthClassNum = lengthClassNum;
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
        return "<age class #" + ageClassNum + " " + ageRange + ", length class #" + lengthClassNum + " " + lengthRange + ">";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AgeLengthClass that = (AgeLengthClass) o;

        if (ageClassNum != that.ageClassNum) return false;
        if (lengthClassNum != that.lengthClassNum) return false;
        if (!ageRange.equals(that.ageRange)) return false;
        return lengthRange.equals(that.lengthRange);

    }

    @Override
    public int hashCode() {
        int result = ageClassNum;
        result = 31 * result + lengthClassNum;
        result = 31 * result + ageRange.hashCode();
        result = 31 * result + lengthRange.hashCode();
        return result;
    }
}
