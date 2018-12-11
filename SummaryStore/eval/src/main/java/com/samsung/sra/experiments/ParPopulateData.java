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

import com.samsung.sra.datastore.SummaryStore;
import com.samsung.sra.datastore.ingest.CountBasedWBMH;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.Semaphore;

public class ParPopulateData {
    private static final Logger logger = LoggerFactory.getLogger(ParPopulateData.class);

    private static final int WINDOW_MERGE_POOL_SIZE = 10;
    private static final int WINDOW_MERGE_FREQUENCY = 100_000;

    private static void syntaxError() {
        System.err.println("SYNTAX: ParPopulateData config.toml [shardNum]");
        System.exit(2);
    }

    public static void main(String[] args) throws Exception {
        File configFile;
        if (args.length < 1 || !(configFile = new File(args[0])).isFile()) {
            syntaxError();
            return;
        }
        Configuration config = new Configuration(configFile);
        long nStreams = config.getNStreams();
        long nStreamsPerShard = config.getNStreamsPerShard();
        long shardNum;
        if (nStreams > nStreamsPerShard) {
            if (args.length != 2) syntaxError();
            shardNum = Long.parseLong(args[1]);
            assert 0 <= shardNum && shardNum < config.getNShards() : "Invalid shard number";
        } else {
            shardNum = 0;
        }
        Semaphore sem = new Semaphore(config.getNumIngestThreads());

        config.getDecayFunctions().forEach(decay -> {
            String outdir = config.getStoreDirectory(decay);
            if (nStreams > nStreamsPerShard) {
                outdir += ".shard" + shardNum;
            }
            if ((new File(outdir).exists())) {
                logger.warn("Decay function {} already populated at {}, skipping", decay, outdir);
                return;
            }
            try (SummaryStore store = new SummaryStore(outdir, new SummaryStore.StoreOptions()
                    .setSharedWindowMergePool(WINDOW_MERGE_POOL_SIZE))) {
                long S0 = shardNum * nStreamsPerShard;
                long Se = Math.min(S0 + nStreamsPerShard - 1, nStreams - 1);
                int nThreads = (int) (Se - S0 + 1);
                StreamWriter[] writers = new StreamWriter[nThreads];
                Thread[] writerThreads = new Thread[nThreads];
                for (int i = 0; i < nThreads; ++i) {
                    writers[i] = new StreamWriter(store, sem, S0 + i, decay, config);
                    writerThreads[i] = new Thread(writers[i], i + "-appender");
                }
                for (int i = 0; i < nThreads; ++i) {
                    writerThreads[i].start();
                }
                for (int i = 0; i < nThreads; ++i) {
                    writerThreads[i].join();
                }
                store.loadStream(S0);
                logger.info("{}: {} windows in stream {}", outdir, store.getNumSummaryWindows(S0), S0);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static class StreamWriter implements Runnable {
        private final long streamID;
        private final String decay;
        private final Configuration conf;
        private final SummaryStore store;
        private final Semaphore semaphore;

        private StreamWriter(SummaryStore store, Semaphore semaphore, long streamID, String decay, Configuration conf)
                throws Exception {
            this.store = store;
            this.semaphore = semaphore;
            this.streamID = streamID;
            this.decay = decay;
            this.conf = conf;
        }

        @Override
        public void run() {
            semaphore.acquireUninterruptibly();
            SummaryStore.StreamOptions streamOptions = new SummaryStore.StreamOptions()
                    .setValuesAreLongs(true)
                    .setIngestBufferSize(conf.getIngestBufferSize())
                    .setWindowMergeFrequency(WINDOW_MERGE_FREQUENCY);
            try {
                store.registerStream(streamID, streamOptions, conf.parseDecayFunction(decay), conf.getOperators());
                ParRandomStreamIterator ris = conf.getParStreamIterator(streamID);
                while (ris.hasNext()) {
                    store.append(streamID, ris.currT, ris.currV);
                    ris.next();
                }
                wbmh.flushAndSetUnbuffered();
                logger.info("Populated stream {}", streamID);
                store.unloadStream(streamID);
                logger.info("Unloaded stream {}", streamID);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                semaphore.release();
            }
        }
    }
}
