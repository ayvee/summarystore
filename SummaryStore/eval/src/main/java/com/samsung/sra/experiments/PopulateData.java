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

import com.samsung.sra.datastore.ingest.CountBasedWBMH;
import com.samsung.sra.datastore.SummaryStore;
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

        // uncomment the parallelStream to parallelize
        config.getDecayFunctions()./*parallelStream().*/forEach(decay -> {
            String outprefix = config.getStoreDirectory(decay);
            if ((new File(outprefix).exists())) {
                logger.warn("Decay function {} already populated at {}, skipping", decay, outprefix);
                return;
            }
            try (SummaryStore store = new SummaryStore(outprefix/*, config.getWindowCacheSize()*/);
                 StreamGenerator streamgen = config.getStreamGenerator()) {
                // FIXME: push constants into config file
                CountBasedWBMH wbmh = new CountBasedWBMH(config.parseDecayFunction(decay))
                        .setValuesAreLongs(true)
                        .setBufferSize(config.getIngestBufferSize())
                        .setWindowsPerMergeBatch(100_000)
                        .setParallelizeMerge(10);
                store.registerStream(streamID, wbmh, config.getOperators());
                streamgen.reset();
                long[] N = {0};
                streamgen.generate(config.getTstart(), config.getTend(), op -> {
                    try {
                        switch (op.type) {
                            case APPEND:
                                if (++N[0] % 10_000_000 == 0) {
                                    logger.info("Inserted {} elements", String.format("%,d", N[0]));
                                }
                                store.append(streamID, op.timestamp, op.value);
                                break;
                            case LANDMARK_START:
                                store.startLandmark(streamID, op.timestamp);
                                break;
                            case LANDMARK_END:
                                store.endLandmark(streamID, op.timestamp);
                                break;
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                wbmh.flushAndSetUnbuffered();
                //store.flush(streamID);
                logger.info("Inserted {} elements", N[0]);
                logger.info("{} = {} windows", outprefix, store.getNumSummaryWindows(streamID));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
