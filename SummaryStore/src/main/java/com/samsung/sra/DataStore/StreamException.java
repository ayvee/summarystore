package com.samsung.sra.DataStore;

public class StreamException extends Exception {
    public StreamException(String msg) {
        super(msg);
    }

    public StreamException(String msg, Throwable t) {
        super(msg, t);
    }

    public StreamException(Throwable t) {
        super(t);
    }
}
