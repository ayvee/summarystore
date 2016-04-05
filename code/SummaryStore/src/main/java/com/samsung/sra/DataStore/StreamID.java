package com.samsung.sra.DataStore;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class StreamID implements Serializable {
    private final int id;

    public StreamID(int id) {
        this.id = id;
    }

    /**
     * How many bytes long is a StreamID?
     */
    static final int byteCount = 4;

    /**
     * put id into buffer. Like all ByteBuffer puts, this advances the buffer position
     */
    void writeToByteBuffer(ByteBuffer buffer) {
        buffer.putInt(id);
    }

    /**
     * get id from buffer. Like all ByteBuffer gets, this advances the buffer position
     */
    static StreamID readFromByteBuffer(ByteBuffer buffer) {
        return new StreamID(buffer.getInt());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StreamID streamID = (StreamID) o;

        return id == streamID.id;

    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public String toString() {
        return Integer.toString(id);
    }
}
