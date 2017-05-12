package com.samsung.sra.DataStore.Storage;

public class BackingStoreException extends Exception {
    public BackingStoreException(String msg) {
        super(msg);
    }

    public BackingStoreException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public BackingStoreException(Throwable cause) {
        super(cause);
    }
}
