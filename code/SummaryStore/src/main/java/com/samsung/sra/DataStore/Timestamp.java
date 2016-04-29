package com.samsung.sra.DataStore;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class Timestamp implements Comparable<Timestamp>, Serializable {
    final long value;

    public Timestamp(long value) {
        this.value = value;
    }

    /**
     * How many bytes long is a Timestamp?
     */
    static final int byteCount = 8;

    /**
     * put id into buffer. Like all ByteBuffer puts, this advances the buffer position
     */
    void writeToByteBuffer(ByteBuffer buffer) {
        buffer.putLong(value);
    }

    /**
     * get id from buffer. Like all ByteBuffer gets, this advances the buffer position
     */
    static Timestamp readFromByteBuffer(ByteBuffer buffer) {
        return new Timestamp(buffer.getLong());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Timestamp streamID = (Timestamp) o;

        return value == streamID.value;

    }

    @Override
    public int hashCode() {
        return Long.hashCode(value);
    }

    @Override
    public String toString() {
        return Long.toString(value);
    }

    public int compareTo(Timestamp that) {
        if (that == null) {
            throw new NullPointerException("comparing null timestamp");
        }
        return (int)(this.value - that.value);
    }
}
