package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.*;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;

class WriteLoadGenerator {
    private static Logger logger = LoggerFactory.getLogger(WriteLoadGenerator.class);
    private final InterarrivalDistribution interarrivalDistribution;
    private final ValueDistribution valueDistribution;
    private final long streamID;
    private final Collection<SummaryStore> datastores;
    private long T = 0;

    WriteLoadGenerator(InterarrivalDistribution interarrivalDistribution, ValueDistribution valueDistribution,
                       long streamID, Collection<SummaryStore> datastores) throws RocksDBException, StreamException {
        this.interarrivalDistribution = interarrivalDistribution;
        this.valueDistribution = valueDistribution;
        this.streamID = streamID;
        this.datastores = datastores;
    }

    WriteLoadGenerator(InterarrivalDistribution interarrivalDistribution, ValueDistribution valueDistribution,
                       long streamID, SummaryStore... datastores) throws RocksDBException, StreamException {
        this(interarrivalDistribution, valueDistribution, streamID, Arrays.asList(datastores));
    }

    void generateUntil(long Tmax) throws StreamException, RocksDBException {
        // TODO: parallelize appends to one thread per store
        for (; T <= Tmax; T += interarrivalDistribution.getNextInterarrival()) {
            if (T % 1_000_000 == 0) {
                logger.debug("appending value #{}", T);
            }
            long ts = T;
            long value = valueDistribution.getNextValue();
            for (DataStore ds: datastores) {
                ds.append(streamID, ts, value);
            }
        }
    }
}
