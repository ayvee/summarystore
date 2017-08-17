package com.samsung.sra.experiments;

import com.moandjiezana.toml.Toml;
import com.samsung.sra.datastore.SummaryStore;
import com.samsung.sra.datastore.ingest.CountBasedWBMH;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ThreadLocalRandom;

public class SimplePopulateData {
    private static final long streamID = 0;
    private static final Logger logger = LoggerFactory.getLogger(SimplePopulateData.class);

    public static void main(String[] args) throws Exception {
        File configFile;
        if (args.length != 1 || !(configFile = new File(args[0])).isFile()) {
            System.err.println("SYNTAX: PopulateData config.toml");
            System.exit(2);
            return;
        }
        Configuration config = new Configuration(configFile);

        // uncomment the parallelStream to parallelize
        config.getDecayFunctions()./*parallelStream().*/forEach(decay -> {
            String outprefix = config.getStorePrefix(decay);
            if ((new File(outprefix + ".backingStore").exists())) {
                logger.warn("Decay function {} already populated at {}.backingStore, skipping", decay, outprefix);
                return;
            }
            try (SummaryStore store = new SummaryStore(outprefix)) {
                // FIXME: push constants into config file
                CountBasedWBMH wbmh = new CountBasedWBMH(config.parseDecayFunction(decay))
                        .setValuesAreLongs(true)
                        .setBufferSize(config.getIngestBufferSize())
                        .setWindowsPerMergeBatch(100_000)
                        .setParallelizeMerge(10);
                Toml data = config.getToml().getTable("data");
                assert data.getString("stream-generator").equals("RandomStreamGenerator");
                Distribution<Long> interarrivals = Configuration.parseDistribution(data.getTable("interarrivals")),
                        values = Configuration.parseDistribution(data.getTable("values"));
                store.registerStream(streamID, wbmh, config.getOperators());
                ThreadLocalRandom r = ThreadLocalRandom.current();
                r.setSeed(0L); // FIXME
                long N = 0;
                for (long t = config.getTstart(); t <= config.getTend(); ) {
                    if (++N % 10_000_000 == 0) logger.info("Inserted {} elements", String.format("%,d", N));
                    long v = values.next(r);
                    store.append(streamID, t, v);
                    t += interarrivals.next(r);
                }
                wbmh.flushAndSetUnbuffered();
                //store.flush(streamID);
                logger.info("Inserted {} elements", N);
                logger.info("{} = {} windows", outprefix, store.getNumSummaryWindows(streamID));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
