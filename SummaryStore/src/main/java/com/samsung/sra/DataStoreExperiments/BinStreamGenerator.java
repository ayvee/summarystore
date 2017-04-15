package com.samsung.sra.DataStoreExperiments;

import com.moandjiezana.toml.Toml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.function.Consumer;

/**
 * Replay stream from a binary file. Loads entire file into memory, supports creating shallow copy objects.
 * Binary format = long arrays serialized to disk. Can only hold 2 billion eles.
 * TODO: eventually take a standard scientific binary format instead, like HDF. */
public class BinStreamGenerator implements StreamGenerator {
    private static final Logger logger = LoggerFactory.getLogger(BinStreamGenerator.class);
    private long[] ts, vs;
    private int N;

    public BinStreamGenerator(Toml params) throws IOException, ClassNotFoundException {
        this(params.getString("file"));
    }

    private BinStreamGenerator(long[] ts, long[] vs, int N) {
        this.ts = ts;
        this.vs = vs;
        this.N = N;
    }

    public BinStreamGenerator(String filename) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filename))) {
            ts = (long[]) ois.readObject();
            vs = (long[]) ois.readObject();
            assert ts.length == vs.length;
            N = ts.length;
        }
    }

    @Override
    public void generate(long T0, long T1, Consumer<Operation> consumer) throws IOException {
        for (int i = 0; i < N; ++i) {
            long t = ts[i], v = vs[i];
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
        return new BinStreamGenerator(ts, vs, N);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("SYNTAX: BinStreamGenerator <file_to_print.pbin>");
            System.exit(2);
        }
        try (BinStreamGenerator psg = new BinStreamGenerator(args[0])) {
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
