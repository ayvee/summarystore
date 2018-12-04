/*
* Copyright 2016 Samsung Research America. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.samsung.sra.experiments;

import org.apache.commons.math3.util.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
        // TODO: rejection sampling is wasteful in very sparse bins. But not an issue in our experiments so far
        while (true) {
            long age = ageBin.sample(random);
            long length = lengthBin.sample(random);
            if (maxAge == null || age + length - 1 <= maxAge) {
                return new Pair<>(age, length);
            }
        }
    }

    /** Get all possible age/length combinations */
    public Collection<Pair<Long, Long>> getAllAgeLengths() {
        List<Pair<Long, Long>> ret = new ArrayList<>();
        for (long a = ageBin.multiplier * ageBin.start; a <= ageBin.multiplier * ageBin.end; a += ageBin.multiplier) {
            for (long l = lengthBin.multiplier * lengthBin.start; l <= lengthBin.multiplier * lengthBin.end; l += lengthBin.multiplier) {
                if (maxAge == null || a + l - 1 <= maxAge) {
                    ret.add(new Pair<>(a, l));
                }
            }
        }
        return ret;
    }

    @Override
    public String toString() {
        return String.format("%s\t%s", ageBin, lengthBin);
    }
}
