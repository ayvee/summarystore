package com.samsung.sra.DataStore;

import com.samsung.sra.DataStore.Storage.BackingStoreException;
import com.samsung.sra.DataStore.Storage.StreamWindowManager;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;

/**
 * Encapsulates code for EH/WBMH/similar mechanisms
 */
public interface WindowingMechanism extends Serializable {
    /**
     * Process the append: update the set of decayed summary windows in the store (merge existing windows and/or
     * create new windows), and insert the provided value into the appropriate window.
     *
     * We will maintain a separate WindowingMechanism object per stream (streamID will be provided
     * to the constructor), so implementors can store stream-specific state. Also, implementors do
     * not need to worry about concurrency, all writes will be serialized before this function is
     * invoked.
     */
    void append(StreamWindowManager windows, long ts, Object[] value) throws BackingStoreException;

    // deserialization hook
    default void populateTransientFields(ExecutorService executorService) {}

    void flush(StreamWindowManager windows) throws BackingStoreException;

    void close(StreamWindowManager windowManager) throws BackingStoreException;
}
