package com.samsung.sra.DataStore;

public class StreamException extends Exception {
    public StreamException(String msg) {
        super(msg);
    }

    public StreamException(Throwable t) {
        super(t);
    }
}
