package com.samsung.sra.DataStoreExperiments;

import com.moandjiezana.toml.Toml;
import com.samsung.sra.DataStore.Aggregates.SimpleCountOperator;
import com.samsung.sra.DataStore.CountBasedWBMH;
import com.samsung.sra.DataStore.RationalPowerWindowing;
import com.samsung.sra.DataStore.SummaryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;

/** Replay a CSV/TSV file to generate a stream. Does not load into memory, leaves it on disk */
public class CSVStreamGenerator implements StreamGenerator {
    private static Logger logger = LoggerFactory.getLogger(StreamGenerator.class);
    private final String traceFile;
    private final String separator;
    private final int tsIndex, valIndex;

    private BufferedReader traceReader;

    public CSVStreamGenerator(Toml params) throws IOException {
        this(params.getString("file"));
    }

    public CSVStreamGenerator(String traceFile, String separator, int tsIndex, int valIndex) throws IOException {
        this.traceFile = traceFile;
        this.separator = separator;
        this.tsIndex = tsIndex;
        this.valIndex = valIndex;
        reset();
    }

    public CSVStreamGenerator(String traceFile) throws IOException {
        this(traceFile, ",", 0, 1);
    }

    private Long currTimestamp;
    private Object currValue = null;

    private void readNextLine() throws IOException {
        while (true) {
            String line = traceReader.readLine();
            if (line == null) {
                currTimestamp = null;
                currValue = null;
                break;
            } else {
                if (line.isEmpty() || line.startsWith("#")) continue;
                String[] vals = line.split(separator);
                assert vals.length > tsIndex && vals.length > valIndex : "incomplete line " + line;
                long newTimestamp = Long.parseLong(vals[tsIndex]);
                if (currTimestamp == null || newTimestamp != currTimestamp) {
                    currTimestamp = newTimestamp;
                    currValue = Long.parseLong(vals[valIndex]);
                    break;
                }
            }
        }
    }

    @Override
    public void generate(long T0, long T1, Consumer<Operation> consumer) throws IOException {
        while (currTimestamp != null && currTimestamp >= T0 && currTimestamp <= T1) {
            consumer.accept(new Operation(Operation.Type.APPEND, currTimestamp, currValue));
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
        StreamGenerator generator = new CSVStreamGenerator(
                "/Users/a.vulimiri/samsung/summarystore/code/workloads/google-cluster-data/task_event_count");
        long ts = System.currentTimeMillis();
        for (int i = 0; i < 1; ++i) {
            long baseT = i * 2506199602822L;
            generator.generate(0, Long.MAX_VALUE, op -> {
                assert op.type == Operation.Type.APPEND;
                try {
                    store.append(streamID, baseT + op.timestamp, op.value);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            logger.info("finished appending month {}", i + 1);
            generator.reset();
        }
        store.flush(streamID);
        long te = System.currentTimeMillis();
        System.out.println("Write throughput = " +
                (store.getStreamStatistics(streamID).getNumValues() * 1000d / (double)(te - ts)) + " per second");
        store.printWindowState(streamID);
        System.out.println(store.query(streamID, (long)600e6, (long)900e6, 0, 0.95));
    }
}
