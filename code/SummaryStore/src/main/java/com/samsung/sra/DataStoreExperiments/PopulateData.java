package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.*;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.RandomGeneratorFactory;

import java.util.Random;

// TODO: error recovery
public class PopulateData {
    private static final long streamID = 0;

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("N", "size of stream to generate (number of elements)").withRequiredArg().ofType(Long.class).required();
        parser.accepts("W", "number of windows").withRequiredArg().ofType(Long.class).required();
        parser.accepts("decay", "decay function").withRequiredArg().required();
        parser.accepts("outprefix", "prefix to store output files under").withRequiredArg().required();

        long N, W;
        String decay;
        WindowLengths windowLengths;
        String outprefix;
        try {
            OptionSet options = parser.parse(args);
            N = (long)options.valueOf("N");
            W = (long)options.valueOf("W");
            if (N <= 0) {
                throw new IllegalArgumentException("N should be positive");
            }
            if (W > N) {
                throw new IllegalArgumentException("W should be <= N");
            }
            windowLengths = null;
            decay = (String) options.valueOf("decay");
            if (decay.equals("exponential")) {
                windowLengths = ExponentialWindowLengths.getWindowingOfSize(N, W);
            } else {
                for (int d = 0; d < 10; ++d) {
                    if (decay.equals("polynomial" + d)) {
                        windowLengths = PolynomialWindowLengths.getWindowingOfSize(d, N, W);
                        break;
                    }
                }
            }
            if (windowLengths == null) {
                throw new IllegalArgumentException("unrecognized decay function");
            }
            outprefix = (String)options.valueOf("outprefix");
        } catch (IllegalArgumentException | OptionException e) {
            System.err.println("ERROR: " + e.getMessage());
            parser.printHelpOn(System.err);
            System.exit(2);
            return;
        }

        populateData(outprefix, N, W, decay, windowLengths);
    }

    private static void populateData(String prefix, long N, long W, String decayName, WindowLengths windowLengths) throws Exception {
        SummaryStore store = new SummaryStore(prefix);
        store.registerStream(streamID, new CountBasedWBMH(streamID, windowLengths));
        // NOTE: fixing seed at 0 here, guarantees that every decay function will see the same set of values
        RandomGenerator rng = RandomGeneratorFactory.createRandomGenerator(new Random(0));
        InterarrivalDistribution interarrivals = new FixedInterarrival(1);
        ValueDistribution values = new UniformValues(rng, 0, 100);
        WriteLoadGenerator generator = new WriteLoadGenerator(interarrivals, values, streamID, store);
        generator.generateUntil(N);
        store.close();
    }
}
