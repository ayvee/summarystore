package com.samsung.sra.datastore.storage;

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
