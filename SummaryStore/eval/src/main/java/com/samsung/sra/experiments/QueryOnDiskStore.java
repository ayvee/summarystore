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

public class QueryOnDiskStore {
    private static final long streamID = 0;

    public static void main(String[] args) throws Exception {
        String prefix;
        long l, r;
        try {
            if (args.length < 1) {
                throw new IllegalArgumentException("missing argument");
            }
            prefix = args[0];
            if (args.length > 1 && args.length != 3) {
                throw new IllegalArgumentException("wrong number of arguments");
            } else if (args.length == 3) {
                // Long.parseLong throws NumberFormatException on failure, which subclasses IllegalArgumentException
                l = Long.parseLong(args[1]);
                r = Long.parseLong(args[2]);
            } else {
                l = 0;
                r = -1;
            }
        } catch (IllegalArgumentException e) {
            System.err.println("SYNTAX ERROR: " + e.getMessage());
            System.err.println("\tQueryOnDiskStore <store_location_prefix> [queryL] [queryR]");
            System.exit(2);
            return;
        }

        SummaryStore store = new SummaryStore(prefix);
        if (r == -1) {
            r = store.getStreamStatistics(streamID).getNumValues() - 1;
        }
        store.printWindowState(streamID);
        System.out.println(
                "Store size = " + store.getNumSummaryWindows(streamID) + " windows; " +
                "COUNT[" + l + ":" + r + "] = " + store.query(streamID, l, r, 0));
    }
}
