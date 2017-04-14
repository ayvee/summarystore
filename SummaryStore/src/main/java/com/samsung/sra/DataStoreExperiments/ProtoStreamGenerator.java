package com.samsung.sra.DataStoreExperiments;

import com.moandjiezana.toml.Toml;
import com.samsung.sra.protocol.TimeSeriesOuterClass.TimeSeries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

/** Replay stream from a protobuf binar file. Loads entire file into memory, supports creating shallow copy objects */
public class ProtoStreamGenerator implements StreamGenerator {
    private static final Logger logger = LoggerFactory.getLogger(ProtoStreamGenerator.class);
    private List<Long> ts, vs;
    private int N = 0;

    public ProtoStreamGenerator(Toml params) throws IOException {
        this(params.getString("file"));
    }

    private ProtoStreamGenerator(List<Long> ts, List<Long> vs, int N) {
        this.ts = ts;
        this.vs = vs;
        this.N = N;
    }

    public ProtoStreamGenerator(String filename) throws IOException {
        TimeSeries series;
        try (FileInputStream is = new FileInputStream(filename)) {
            series = TimeSeries.parseFrom(is);
        }
        ts = series.getTimestampList();
        vs = series.getValueList();
        N = ts.size();
        assert N == vs.size();
    }

    @Override
    public void generate(long T0, long T1, Consumer<Operation> consumer) throws IOException {
        for (int i = 0; i < N; ++i) {
            long t = ts.get(i), v = vs.get(i);
            if (t >= T0) {
                if (t <= T1) {
                    consumer.accept(new Operation(Operation.Type.APPEND, t, new Object[]{v}));
                } else {
                    break;
                }
            }
        }
    }

    @Override
    public void reset() throws IOException {
    }

    @Override
    public boolean isCopyable() {
        return true;
    }

    @Override
    public StreamGenerator copy() {
        return new ProtoStreamGenerator(ts, vs, N);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("SYNTAX: ProtoStreamGenerator <file_to_print.pbin>");
            System.exit(2);
        }
        try (ProtoStreamGenerator psg = new ProtoStreamGenerator(args[0])) {
            for (int i = 0; i < 2; ++i) {
                System.out.printf("Run %d\n", i);
                psg.generate(0, Long.MAX_VALUE, op -> {
                    assert op.type == Operation.Type.APPEND;
                    System.out.printf("\t%d,%d\n", op.timestamp, (long) op.value[0]);
                });
                psg.reset();
            }
        }
    }
}
