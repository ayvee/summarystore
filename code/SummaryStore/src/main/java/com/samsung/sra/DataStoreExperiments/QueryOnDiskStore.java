package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.QueryType;
import com.samsung.sra.DataStore.SummaryStore;

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
            r = store.getStreamCount(streamID) - 1;
        }
        store.printBucketState(streamID);
        System.out.println(
                "Store size = " + store.getStoreSizeInBytes() + " bytes; " +
                "COUNT[" + l + ":" + r + "] = " + store.query(streamID, l, r, QueryType.COUNT, null));
    }
}
