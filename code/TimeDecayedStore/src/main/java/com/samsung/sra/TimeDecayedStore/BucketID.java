package com.samsung.sra.TimeDecayedStore;

import java.nio.ByteBuffer;

/**
 * Created by a.vulimiri on 1/19/16.
 */
public class BucketID implements Comparable<BucketID> {
    private final int id;

    public BucketID(int id) {
        this.id = id;
    }

    public BucketID nextBucketID() {
        return new BucketID(id + 1);
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
    static BucketID readFromByteBuffer(ByteBuffer buffer) {
        return new BucketID(buffer.getInt());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BucketID streamID = (BucketID) o;

        return id == streamID.id;

    }

    @Override
    public int hashCode() {
        return id;
    }

    public int compareTo(BucketID that) {
        if (that == null) {
            throw new NullPointerException("comparing null BucketID");
        }
        return this.id - that.id;
    }
}
