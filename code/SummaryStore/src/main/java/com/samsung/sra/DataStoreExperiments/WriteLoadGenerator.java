package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.*;
import org.rocksdb.RocksDBException;

import java.text.DateFormat;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;

class WriteLoadGenerator {
    private final InterarrivalDistribution interarrivalDistribution;
    private final ValueDistribution valueDistribution;
    private final StreamID streamID;
    private final Collection<SummaryStore> datastores;
    private long T = 0;

    WriteLoadGenerator(InterarrivalDistribution interarrivalDistribution, ValueDistribution valueDistribution,
                       StreamID streamID, Collection<SummaryStore> datastores) throws RocksDBException, StreamException {
        this.interarrivalDistribution = interarrivalDistribution;
        this.valueDistribution = valueDistribution;
        this.streamID = streamID;
        this.datastores = datastores;
    }

    WriteLoadGenerator(InterarrivalDistribution interarrivalDistribution, ValueDistribution valueDistribution,
                       StreamID streamID, SummaryStore... datastores) throws RocksDBException, StreamException {
        this(interarrivalDistribution, valueDistribution, streamID, Arrays.asList(datastores));
    }

    void generateUntil(long Tmax) throws StreamException, RocksDBException {
        for (; T <= Tmax; T += interarrivalDistribution.getNextInterarrival()) {
            Timestamp ts = new Timestamp(T);
            long value = valueDistribution.getNextValue();
            for (DataStore ds: datastores) {
                ds.append(streamID, ts, value);
            }
        }
    }
}
