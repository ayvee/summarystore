package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.*;
import com.samsung.sra.DataStore.Aggregates.SimpleCountOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.BiConsumer;

/** TODO: turn this into an arbitrary trace player eventually */
public class GoogleStreamGenerator implements StreamGenerator, AutoCloseable {
    private static Logger logger = LoggerFactory.getLogger(StreamGenerator.class);
    private final String traceFile;

    private BufferedReader traceReader;

    public GoogleStreamGenerator(String traceFile) throws IOException {
        this.traceFile = traceFile;
        reset();
    }

    private Long currTimestamp;
    private Long currValue;

    private void readNextLine() throws IOException {
        while (true) {
            String line = traceReader.readLine();
            if (line == null) {
                currTimestamp = null;
                currValue = null;
                break;
            } else {
                String[] vals = line.split(",");
                long newTimestamp = Long.parseLong(vals[0]);
                if (currTimestamp == null || newTimestamp != currTimestamp) {
                    currTimestamp = newTimestamp;
                    currValue = Long.parseLong(vals[1]);
                    break;
                }
            }
        }
    }

    @Override
    public void generate(long T, BiConsumer<Long, Long> consumer) throws IOException {
        while (currTimestamp != null && currTimestamp < T) {
            consumer.accept(currTimestamp, currValue);
            readNextLine();
        }
    }

    @Override
    public void reset() throws IOException {
        if (traceReader != null) traceReader.close();
        traceReader = Files.newBufferedReader(Paths.get(traceFile));
        readNextLine();
    }

    @Override
    public void close() throws Exception {
        if (traceReader != null) traceReader.close();
    }

    public static void main(String[] args) throws Exception {
        String prefix = "/tmp/tdstore/googletrace_test_";
        long streamID = 0;
        Runtime.getRuntime().exec(new String[]{"sh", "-c", "rm -rf " + prefix + "*"}).waitFor();
        SummaryStore store = new SummaryStore("/tmp/googletrace_test_");
        store.registerStream(streamID,
                new CountBasedWBMH(new RationalPowerWindowing(1, 1, 6, 1), 2_000_000),
                new SimpleCountOperator());
        StreamGenerator generator = new GoogleStreamGenerator(
                "/Users/a.vulimiri/samsung/summarystore/code/workloads/google-cluster-data/task_event_count");
        long ts = System.currentTimeMillis();
        for (int i = 0; i < 12; ++i) {
            long baseT = i * 2506199602822L;
            generator.generate(Long.MAX_VALUE, (t, v) -> {
                try {
                    store.append(streamID, baseT + t, v);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            generator.reset();
        }
        store.flush(streamID);
        long te = System.currentTimeMillis();
        System.out.println("Write throughput = " +
                (store.getStreamStatistics(streamID).getTotalCount() * 1000d / (double)(te - ts)) + " per second");
        store.printBucketState(streamID);
        System.out.println(store.query(streamID, (long)600e6, (long)900e6, 0, 0.95));
    }
}
