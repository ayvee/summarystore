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
        ArgumentType<Long> CommaSeparatedLong = (argParser, arg, value) -> Long.valueOf(value.replace(",", ""));
        parser.addArgument("outdir").help("output directory");
        parser.addArgument("T").help("size of stream to generate").type(CommaSeparatedLong);
        parser.addArgument("D").help("decay function [" + CLIParser.getValidDecayFunctions() + "]");
        parser.addArgument("-I")
                .help("interarrival distribution [" + CLIParser.getValidInterarrivalDistributions() + "]")
                .setDefault("fixed1");
        parser.addArgument("-V")
                .help("value distribution [" + CLIParser.getValidValueDistributions() + "]")
                .setDefault("uniform0,100");
        parser.addArgument("-R").help("stream generator RNG seed").type(Long.class).setDefault(0L);
        parser.addArgument("-cachesize")
                .help("number of buckets per stream to cache in main memory").metavar("CS")
                .type(CommaSeparatedLong).setDefault(0L);

        String outdir;
        long T;
        String I, V, D;
        InterarrivalDistribution interarrivals;
        ValueDistribution values;
        Windowing windowing;
        long R;
        long cacheSize;
        try {
            Namespace parsed = parser.parseArgs(args);
            outdir = parsed.get("outdir");
            T = parsed.get("T");
            if (T <= 0) {
                throw new IllegalArgumentException("T should be positive");
            }
            I = parsed.get("I");
            interarrivals = CLIParser.parseInterarrivalDistribution(I);
            V = parsed.get("V");
            values = CLIParser.parseValueDistribution(V);
            D = parsed.get("D");
            windowing = CLIParser.parseDecayFunction(D);
            cacheSize = parsed.get("cachesize");
            R = parsed.get("R");
        } catch (ArgumentParserException | IllegalArgumentException e) {
            System.err.println("ERROR: " + e.getMessage());
            parser.printHelp(new PrintWriter(System.err, true));
            System.exit(2);
            return;
        }

        String outprefix = String.format("%s/T%d.I%s.V%s.R%d.D%s", outdir, T, I, V, R, D);
        StreamGenerator generator = new StreamGenerator(interarrivals, values, R);
        populateData(outprefix, generator, R, T, windowing, cacheSize);
    }

    private static void populateData(String prefix, StreamGenerator streamGenerator, long R, long T, Windowing windowing, long cacheSize) throws Exception {
        SummaryStore store = new SummaryStore(prefix, cacheSize);
        store.registerStream(streamID, new CountBasedWBMH(streamID, windowing));
        // NOTE: fixing random seed at 0 here, guarantees that every experiment will see the same run of values
        streamGenerator.reset(R);
        streamGenerator.generate(T, (t, v) -> {
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
