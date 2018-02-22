package com.samsung.sra.experiments;

import com.moandjiezana.toml.Toml;
import com.samsung.sra.experiments.StreamGenerator.Operation.Type;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Wraps an input stream that only produces values (no landmark events) and identifies landmarks based on simple rules
 */
public class LandmarkTagger implements StreamGenerator {
    private final StreamGenerator baseStream;
    private Long threshold = null;
    private Statistics stats = null;
    private Double numSDs = null;

    public LandmarkTagger(StreamGenerator baseStream, Toml conf) throws IOException {
        this.baseStream = baseStream;
        threshold = conf.getLong("threshold", null);
        numSDs = conf.getDouble("numSDs", null);
        assert exactlyOne(threshold != null, numSDs != null);
        reset();
    }

    /** Check if exactly one of the arguments is true */
    private static boolean exactlyOne(boolean... args) {
        int sum = 0;
        for (boolean arg: args) {
            sum += arg ? 1 : 0;
        }
        return sum == 1;
    }

    private boolean isLandmark(long value) {
        if (threshold != null) {
            return value >= threshold;
        }
        if (numSDs != null) {
            stats.addObservation(value);
            return Math.abs(value - stats.getMean()) > numSDs * stats.getStandardDeviation();
        }
        throw new IllegalStateException("hit unreachable code in landmark tagger");
    }

    @Override
    public void generate(long T0, long T1, Consumer<Operation> consumer) throws IOException {
        MutableBoolean inLandmark = new MutableBoolean(false);
        MutableLong lastTS = new MutableLong(T0 - 1);
        baseStream.generate(T0, T1, op -> {
            assert op.type == Type.APPEND;
            if (isLandmark((long) op.value)) {
                // create landmark window if not already in one
                if (inLandmark.isFalse()) {
                    inLandmark.setValue(true);
                    long prevTS = lastTS.longValue();
                    assert prevTS < T0 || op.timestamp >= prevTS + 1;
                    consumer.accept(new Operation(Type.LANDMARK_START, prevTS >= T0 ? prevTS + 1 : op.timestamp, null));
                }
            } else { // is not landmark
                // close last landmark window if in one
                if (inLandmark.isTrue()) {
                    inLandmark.setValue(false);
                    assert op.timestamp > lastTS.longValue();
                    consumer.accept(new Operation(Type.LANDMARK_END, op.timestamp - 1, null));
                }
            }
            lastTS.setValue(op.timestamp);
            consumer.accept(op);
        });
    }

    @Override
    public void reset() throws IOException {
        baseStream.reset();
        if (numSDs != null) {
            stats = new Statistics();
        }
    }

    @Override
    public void close() throws Exception {
        baseStream.close();
    }
}
