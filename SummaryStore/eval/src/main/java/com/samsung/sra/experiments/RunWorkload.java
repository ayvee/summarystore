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

import com.samsung.sra.datastore.ResultError;
import com.samsung.sra.datastore.SummaryStore;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Run the configured workload on all decays and print query answers. Does not test against true (enumerated) answers;
 * for that use PopulateWorkload + RunComparison instead
 */
public class RunWorkload {
    private static final long streamID = 0;

    public static void main(String[] args) throws Exception {
        File configFile;
        if (args.length != 1 || !(configFile = new File(args[0])).isFile()) {
            System.err.println("SYNTAX: PopulateData config.toml");
            System.exit(2);
            return;
        }
        Configuration conf = new Configuration(configFile);

        Workload workload = conf.getWorkloadGenerator().generate(conf.getTstart(), conf.getTend());
        System.out.println("#decay\t# summary windows\t# elements in stream\tgroup\tt0\tt1\tresult\terror");
        for (String decay: conf.getDecayFunctions()) {
            try (SummaryStore store = new SummaryStore(conf.getStoreDirectory(decay), new SummaryStore.Options()
                    .setKeepReadIndexes(true)
                    .setReadOnly(true)
                    .setCacheSizePerStream(conf.getWindowCacheSize()))) {
                long nSumWindows = store.getNumSummaryWindows(streamID);
                long nElements = store.getStreamStatistics(streamID).getNumValues();
                for (Map.Entry<String, List<Workload.Query>> entry: workload.entrySet()) {
                    String group = entry.getKey();
                    for (Workload.Query q: entry.getValue()) {
                        Object result, error;
                        if (q.queryType == Workload.Query.Type.MAX_THRESH) {
                            ResultError re = (ResultError) store.query(streamID, q.l, q.r, q.operatorNum, q.params);
                            result = (long) re.result > (long) q.params[0];
                            error = re.error;
                        } else if (q.queryType == Workload.Query.Type.MEAN) {
                            double sum = (double) ((ResultError) store.query(streamID, q.l, q.r, (int) q.params[0], q.params)).result;
                            double count = (double) ((ResultError) store.query(streamID, q.l, q.r, (int) q.params[1], q.params)).result;
                            result = sum / count;
                            error = -1;
                        } else {
                            ResultError re = (ResultError) store.query(streamID, q.l, q.r, q.operatorNum, q.params);
                            result = re.result;
                            error = re.error;
                        }
                        System.out.printf("%s\t%d\t%d\t%s\t%d\t%d\t%s\t%s\n", decay, nSumWindows, nElements, group,
                                q.l, q.r, result, error);
                    }
                }
            }
        }
    }
}
