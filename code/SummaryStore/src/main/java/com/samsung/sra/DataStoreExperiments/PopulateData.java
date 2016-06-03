package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.*;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;

public class PopulateData {
    private static final long streamID = 0;
    private static final Logger logger = LoggerFactory.getLogger(PopulateData.class);

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("PopulateData", false).defaultHelp(true);
        ArgumentType<Long> CommaSeparatedLong = (ArgumentParser argParser, Argument arg, String value) ->
                Long.valueOf(value.replace(",", ""));
        parser.addArgument("N").help("size of stream to generate (number of elements)").type(CommaSeparatedLong);
        parser.addArgument("decay").help("decay function");
        parser.addArgument("outprefix").help("prefix to store output files under");
        parser.addArgument("-cachesize").help("number of buckets per stream to cache in main memory").
                type(CommaSeparatedLong).setDefault(0L);

        long N;
        Windowing windowing;
        String outprefix;
        long cacheSize;
        try {
            Namespace parsed = parser.parseArgs(args);
            N = parsed.get("N");
            if (N <= 0) {
                throw new IllegalArgumentException("N should be positive");
            }
            /*long W = (long) parsed.getLong("W");
            if (W > N) {
                throw new IllegalArgumentException("W should be <= N");
            }*/
            String decay = parsed.get("decay");
            if (decay.equals("exponential")) {
                //windowLengths = ExponentialWindowLengths.getWindowingOfSize(N, W);
                windowing = new GenericWindowing(new ExponentialWindowLengths(2));
            } else if (decay.startsWith("rationalPower")) {
                String[] pq = decay.substring("rationalPower".length()).split(",");
                if (pq.length != 2) {
                    throw new IllegalArgumentException("rationalPower decay spec must be rationalPowerp,q");
                }
                int p = Integer.parseInt(pq[0]), q = Integer.parseInt(pq[1]);
                windowing = new RationalPowerWindowing(p, q);
            } else {
                throw new IllegalArgumentException("unrecognized decay function");
            }
            outprefix = parsed.get("outprefix");
            cacheSize = parsed.get("cachesize");
        } catch (ArgumentParserException | IllegalArgumentException e) {
            System.err.println("ERROR: " + e.getMessage());
            parser.printHelp(new PrintWriter(System.err, true));
            System.exit(2);
            return;
        }

        populateData(outprefix, N, windowing, cacheSize);
    }

    private static void populateData(String prefix, long N, Windowing windowing, long cacheSize) throws Exception {
        SummaryStore store = new SummaryStore(prefix, cacheSize);
        store.registerStream(streamID, new CountBasedWBMH(streamID, windowing));
        InterarrivalDistribution interarrivals = new FixedInterarrival(1);
        ValueDistribution values = new UniformValues(0, 100);
        // NOTE: fixing random seed at 0 here, guarantees that every experiment will see the same run of values
        StreamGenerator generator = new StreamGenerator(interarrivals, values, 0);
        generator.generateFor(N, (t, v) -> {
            try {
                store.append(streamID, t, v);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        logger.info("{} = {} bytes", prefix, store.getStoreSizeInBytes());
        store.close();
    }
}
