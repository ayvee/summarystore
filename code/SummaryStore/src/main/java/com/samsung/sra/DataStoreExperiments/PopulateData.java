package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.*;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.RandomGeneratorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class PopulateData {
    private static final long streamID = 0;
    private static final Logger logger = LoggerFactory.getLogger(PopulateData.class);

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("N", "size of stream to generate (number of elements)").withRequiredArg().ofType(Long.class).required();
        //parser.accepts("W", "number of windows").withRequiredArg().ofType(Long.class).required();
        parser.accepts("decay", "decay function").withRequiredArg().required();
        parser.accepts("outprefix", "prefix to store output files under").withRequiredArg().required();

        long N;
        WindowLengths windowLengths;
        String outprefix;
        try {
            OptionSet options = parser.parse(args);
            N = (long) options.valueOf("N");
            //long W = (long) options.valueOf("W");
            if (N <= 0) {
                throw new IllegalArgumentException("N should be positive");
            }
            /*if (W > N) {
                throw new IllegalArgumentException("W should be <= N");
            }*/
            String decay = (String) options.valueOf("decay");
            if (decay.equals("exponential")) {
                //windowLengths = ExponentialWindowLengths.getWindowingOfSize(N, W);
                windowLengths = new ExponentialWindowLengths(2);
            } else if (decay.startsWith("polynomial")) {
                // throws NumberFormatException on parse failure, a subclass of IllegalArgumentException
                int d = Integer.parseInt(decay.substring("polynomial".length()));
                //windowLengths = PolynomialWindowLengths.getWindowingOfSize(d, N, W);
                windowLengths = new PolynomialWindowLengths(1, d);
            } else if (decay.startsWith("rationalPower")) {
                String[] pq = decay.substring("rationalPower".length()).split(",");
                if (pq.length != 2) {
                    throw new IllegalArgumentException("rationalPower decay spec must be rationalPowerp,q");
                }
                int p = Integer.parseInt(pq[0]), q = Integer.parseInt(pq[1]);
                windowLengths = new RationalPowerWindowLengths(p, q);
            } else {
                throw new IllegalArgumentException("unrecognized decay function");
            }
            outprefix = (String)options.valueOf("outprefix");
        } catch (IllegalArgumentException | OptionException e) {
            System.err.println("ERROR: " + e.getMessage());
            parser.printHelpOn(System.err);
            System.exit(2);
            return;
        }

        populateData(outprefix, N, windowLengths);
    }

    private static void populateData(String prefix, long N, WindowLengths windowLengths) throws Exception {
        SummaryStore store = new SummaryStore(prefix);
        store.registerStream(streamID, new CountBasedWBMH(streamID, windowLengths));
        // NOTE: fixing seed at 0 here, guarantees that every experiment will see the same run of values
        RandomGenerator rng = RandomGeneratorFactory.createRandomGenerator(new Random(0));
        InterarrivalDistribution interarrivals = new FixedInterarrival(1);
        ValueDistribution values = new UniformValues(rng, 0, 100);
        WriteLoadGenerator generator = new WriteLoadGenerator(interarrivals, values, streamID, store);
        generator.generateUntil(N);
        logger.info("{} = {} bytes", prefix, store.getStoreSizeInBytes());
        store.close();
    }
}
