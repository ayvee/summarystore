package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.CountBasedWBMH;
import com.samsung.sra.DataStore.SummaryStore;
import com.samsung.sra.DataStore.WindowOperator;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class PopulateData {
    private static final long streamID = 0;
    private static final Logger logger = LoggerFactory.getLogger(PopulateData.class);

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("PopulateData", false).defaultHelp(true);
        parser.addArgument("conf", "config file").type(File.class);
        Configuration config = new Configuration(parser.parseArgs(args).get("conf"));

        long T = config.getT();
        WindowOperator[] operators = config.getOperators();
        long cacheSize = config.getBucketCacheSize();
        try (StreamGenerator streamgen = config.getStreamGenerator()) {
            for (String decay : config.getDecayFunctions()) {
                // Trivially parallelizable (can easily populate different decay funcs in parallel);
                // just need to use a separate streamgen for each store
                String outprefix = config.getStorePrefix() + ".D" + decay;
                try (SummaryStore store = new SummaryStore(outprefix, cacheSize)) {
                    store.registerStream(streamID,
                            new CountBasedWBMH(config.parseDecayFunction(decay), config.getIngestBufferSize()),
                            operators);
                    streamgen.reset();
                    streamgen.generate(T, (t, v) -> {
                        try {
                            store.append(streamID, t, v);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                    logger.info("{} = {} bytes", outprefix, store.getStoreSizeInBytes());
                }
            }
        }
    }
}
