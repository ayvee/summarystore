package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.CountBasedWBMH;
import com.samsung.sra.DataStore.SummaryStore;
import com.samsung.sra.DataStore.WindowOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class PopulateData {
    private static final long streamID = 0;
    private static final Logger logger = LoggerFactory.getLogger(PopulateData.class);

    public static void main(String[] args) throws Exception {
        File configFile;
        if (args.length != 1 || !(configFile = new File(args[0])).isFile()) {
            System.err.println("SYNTAX: PopulateData config.toml");
            System.exit(2);
            return;
        }
        Configuration config = new Configuration(configFile);

        long T = config.getT();
        WindowOperator[] operators = config.getOperators();
        long cacheSize = config.getBucketCacheSize();
        // uncomment the parallelStream to parallelize
        config.getDecayFunctions()./*parallelStream().*/forEach(decay -> {
            String outprefix = config.getStorePrefix() + ".D" + decay;
            if ((new File(outprefix + ".bucketStore").exists())) {
                logger.warn("Decay function {} already populated, skipping", decay);
                return;
            }
            try (SummaryStore store = new SummaryStore(outprefix, cacheSize);
                 StreamGenerator streamgen = config.getStreamGenerator()) {
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
                store.flush(streamID);
                logger.info("{} = {} bytes", outprefix, store.getStoreSizeInBytes());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
