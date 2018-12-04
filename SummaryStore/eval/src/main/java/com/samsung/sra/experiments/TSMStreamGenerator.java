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

import com.moandjiezana.toml.Toml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Consumer;

/**
 * If cmsMode: generate stream of (timestamp, (nodeID, bytes))
 * If !cmsMode: generate stream of (timestamp, bytes)
 */
public class TSMStreamGenerator implements StreamGenerator {
    private static Logger logger = LoggerFactory.getLogger(StreamGenerator.class);
    private final String traceFile;
    private final String separator;
    private final boolean cmsMode;

    private BufferedReader reader;

    private static boolean needCMSMode(List<String> operators) {
        boolean hasCMS = false, hasNonCMS = false;
        for (String opname: operators) {
            if (opname.startsWith("CMSOperator")) {
                hasCMS = true;
            } else {
                hasNonCMS = true;
            }
        }
        if (hasCMS == hasNonCMS) { // NOT (hasCMS XOR hasNoneCMS)
            throw new IllegalArgumentException("TSM does not allow mixing CMS with other operators");
        }
        return hasCMS;
    }

    public TSMStreamGenerator(Toml params) throws IOException {
        this(params.getString("file"), params.getString("separator", ","), needCMSMode(params.getList("operators")));
    }

    public TSMStreamGenerator(String traceFile, String separator, boolean cmsMode) throws IOException {
        this.traceFile = traceFile;
        this.separator = separator;
        this.cmsMode = cmsMode;
        reset();
    }

    @Override
    public void generate(long T0, long T1, Consumer<Operation> consumer) throws IOException {
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            } else if (!line.isEmpty() && !line.startsWith("#")) {
                String[] vals = line.split(separator);
                assert vals.length == 3 : "incomplete line " + line;
                long timestamp = Long.parseLong(vals[0]);
                if (timestamp > T1) {
                    break;
                } else if (timestamp >= T0) {
                    long nodeID = Long.parseLong(vals[1]), bytes = Long.parseLong(vals[2]);
                    Object[] value = cmsMode ? new Object[]{nodeID, bytes} : new Object[]{bytes};
                    consumer.accept(new Operation(Operation.Type.APPEND, timestamp, value));
                } // else if (timestamp < T0) continue
            }
        }
    }

    @Override
    public void reset() throws IOException {
        if (reader != null) reader.close();
        reader = Files.newBufferedReader(Paths.get(traceFile));
    }

    @Override
    public void close() throws Exception {
        if (reader != null) reader.close();
    }
}
