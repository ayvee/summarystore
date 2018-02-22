package com.samsung.sra.experiments;

import com.moandjiezana.toml.Toml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.function.Consumer;

/**
 * Replay stream from a binary file. Loads entire file into memory, supports creating shallow copy objects.
 * Binary format = long arrays serialized to disk. Can only hold ~2 billion (INT32_MAX) eles.
 * TODO: eventually take a standard scientific binary format instead, like HDF. */
public class BinStreamGenerator implements StreamGenerator {
    private static final Logger logger = LoggerFactory.getLogger(BinStreamGenerator.class);
    private long[] ts, vs;
    private int N;
    private int repeat;
    private long ticksPerS;

    public BinStreamGenerator(Toml params) throws IOException, ClassNotFoundException {
        this(params.getString("file"), params.getLong("repeat", 1L).intValue(), params.getLong("ticks-per-second", 1L));
    }

    private BinStreamGenerator(long[] ts, long[] vs, int N, int repeat, long ticksPerS) {
        this.ts = ts;
        this.vs = vs;
        this.N = N;
        this.repeat = repeat;
        this.ticksPerS = ticksPerS;
    }

    public BinStreamGenerator(String filename, int repeat, long ticksPerS) throws IOException, ClassNotFoundException {
        this.repeat = repeat;
        this.ticksPerS = ticksPerS;
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filename))) {
            ts = (long[]) ois.readObject();
            vs = (long[]) ois.readObject();
            assert ts.length == vs.length;
            N = ts.length;
        }
    }

    @Override
    public void generate(long T0, long T1, Consumer<Operation> consumer) throws IOException {
        long base = 0;
        for (int r = 0 ; r < repeat; ++r) {
            for (int i = 0; i < N; ++i) {
                long t = base + ts[i], v = vs[i];
                if (t >= T0) {
                    if (t <= T1) {
                        consumer.accept(new Operation(Operation.Type.APPEND, t, v));
                    } else {
                        break;
                    }
                }
            }
            base += ts[N-1] + ticksPerS;
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
        return new BinStreamGenerator(ts, vs, N, repeat, ticksPerS);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("SYNTAX: BinStreamGenerator <file_to_print.pbin>");
            System.exit(2);
        }
        try (BinStreamGenerator psg = new BinStreamGenerator(args[0], 1, 1)) {
            for (int i = 0; i < 2; ++i) {
                System.out.printf("Run %d\n", i);
                psg.generate(0, Long.MAX_VALUE, op -> {
                    assert op.type == Operation.Type.APPEND;
                    System.out.printf("\t%d,%d\n", op.timestamp, op.value);
                });
                psg.reset();
            }
        }
    }
}
