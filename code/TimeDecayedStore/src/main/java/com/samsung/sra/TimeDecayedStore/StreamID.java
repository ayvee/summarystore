package com.samsung.sra.TimeDecayedStore;

import java.nio.ByteBuffer;

/**
 * Created by a.vulimiri on 1/19/16.
 */
public class StreamID {
    private final int id;

    public StreamID(int id) {
        this.id = id;
    }

    /**
     * How many bytes long is a StreamID?
     * @return
     */
    public static int getByteCount() {
        return 4;
    }

    /**
     * put id into buffer. Like all ByteBuffer puts, this advances the buffer position
     * @param buffer
     */
    void writeToByteBuffer(ByteBuffer buffer) {
        buffer.putInt(id);
    }

    /**
     * get id from buffer. Like all ByteBuffer gets, this advances the buffer position
     * @param buffer
     * @return
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
