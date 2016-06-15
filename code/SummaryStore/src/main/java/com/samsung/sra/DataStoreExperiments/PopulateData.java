package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.CountBasedWBMH;
import com.samsung.sra.DataStore.SummaryStore;
import com.samsung.sra.DataStore.WindowOperator;
import com.samsung.sra.DataStore.Windowing;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.ArgumentType;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.ArrayList;

public class PopulateData {
    private static final long streamID = 0;
    private static final Logger logger = LoggerFactory.getLogger(PopulateData.class);

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("PopulateData", false).defaultHelp(true);
        ArgumentType<Long> CommaSeparatedLong = (argParser, arg, value) -> Long.valueOf(value.replace(",", ""));
        parser.addArgument("outdir").help("output directory");
        parser.addArgument("T").help("size of stream to generate").type(CommaSeparatedLong);
        parser.addArgument("D").help("decay function [" + CLIParser.getValidDecayFunctions() + "]");
        parser.addArgument("operator").nargs("+").help("window operators [" + CLIParser.getValidOperators() + "]");
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
        parser.addArgument("-prefix").help("optional prefix to add to every input/output file").setDefault("");

        String outdir;
        long T;
        String I, V, D;
        ArrayList<WindowOperator> operators = new ArrayList<>();
        InterarrivalDistribution interarrivals;
        ValueDistribution values;
        Windowing windowing;
        long R;
        long cacheSize;
        String prefix;
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
            for (String opName: parsed.<String>getList("operator")) {
                operators.add(CLIParser.parseOperator(opName));
            }
            cacheSize = parsed.get("cachesize");
            R = parsed.get("R");
            prefix = parsed.get("prefix");
        } catch (ArgumentParserException | IllegalArgumentException e) {
            System.err.println("ERROR: " + e.getMessage());
            parser.printHelp(new PrintWriter(System.err, true));
            System.exit(2);
            return;
        }

        String outprefix = String.format("%s/%sT%d.I%s.V%s.R%d.D%s", outdir, prefix, T, I, V, R, D);
        StreamGenerator generator = new StreamGenerator(interarrivals, values, R);
        populateData(outprefix, generator, T, windowing, operators.toArray(new WindowOperator[0]), cacheSize);
    }

    private static void populateData(String prefix, StreamGenerator streamGenerator,
                                     long T, Windowing windowing, WindowOperator[] operators, long cacheSize) throws Exception {
        SummaryStore store = new SummaryStore(prefix, cacheSize);
        store.registerStream(streamID, new CountBasedWBMH(windowing), operators);
        streamGenerator.reset();
        streamGenerator.generate(T, (t, v) -> {
            try {
                store.appendBuf(streamID, t, v);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        logger.info("{} = {} bytes", prefix, store.getStoreSizeInBytes());
        store.close();
    }
}
