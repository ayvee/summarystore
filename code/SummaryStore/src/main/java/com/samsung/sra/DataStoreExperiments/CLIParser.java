package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.*;
import com.samsung.sra.DataStore.Aggregates.SimpleCountOperator;

/**
 * Helper class, constructs appropriate InterarrivalDistribution, ValueDistribution etc
 * objects based on command-line string specifications. Throws IllegalArgumentException
 * on error
 */
class CLIParser {
    static String getValidInterarrivalDistributions() {
        return "fixedVALUE, exponentialLAMBDA";
    }

    static InterarrivalDistribution parseInterarrivalDistribution(String name) {
        if (name == null) {
            throw new IllegalArgumentException("expect non-null interarrival spec");
        } else if (name.startsWith("fixed")) {
            return new FixedInterarrival(Long.parseLong(name.substring("fixed".length())));
        } else if (name.startsWith("exponential")) {
            return new ExponentialInterarrivals(Double.parseDouble(name.substring("exponential".length())));
        } else {
            throw new IllegalArgumentException("unrecognized interarrival spec " + name);
        }
    }

    static String getValidValueDistributions() {
        return "uniformMIN,MAX";
    }

    static ValueDistribution parseValueDistribution(String name) {
        if (name == null) {
            throw new IllegalArgumentException("expect non-null value distribution spec");
        } else if (name.startsWith("uniform")) {
            String[] lr = name.substring("uniform".length()).split(",");
            if (lr.length != 2) throw new IllegalArgumentException("malformed uniform value distribution spec " + name);
            return new UniformValues(Long.parseLong(lr[0]), Long.parseLong(lr[1]));
        } else {
            throw new IllegalArgumentException("unrecognized value distribution spec " + name);
        }
    }

    static String getValidDecayFunctions() {
        return "exponentialBASE, rationalpowerP,Q";
    }

    static Windowing parseDecayFunction(String name) {
        if (name == null) {
            throw new IllegalArgumentException("expect non-null decay spec");
        } else if (name.startsWith("exponential")) {
            return new GenericWindowing(new ExponentialWindowLengths(Double.parseDouble(name.substring("exponential".length()))));
        } else if (name.startsWith("rationalPower")) {
            String[] pq = name.substring("rationalPower".length()).split(",");
            if (pq.length != 2) throw new IllegalArgumentException("malformed rationalPower decay spec " + name);
            return new RationalPowerWindowing(Integer.parseInt(pq[0]), Integer.parseInt(pq[1]));
        } else {
            throw new IllegalArgumentException("unrecognized decay function " + name);
        }

    }

    static String getValidOperators() {
        return "simplecountESTIMATOR";
    }

    static WindowOperator parseOperator(String name) {
        if (name == null) {
            throw new IllegalArgumentException("expect non-null operator spec");
        } else if (name.startsWith("simplecount")) {
            return new SimpleCountOperator(SimpleCountOperator.Estimator.valueOf(name.substring("simplecount".length())));
        } else {
            throw new IllegalArgumentException("unrecognized window operator " + name);
        }
    }
}
