package com.samsung.sra.DataStore.Ingest;

import com.samsung.sra.DataStore.Storage.StreamWindowManager;

import java.io.Serializable;

/**
 * Implements WBMH. Will be given a BlockingQueue<WindowInfo> in constructor which it is expected to process in run()
 */
interface Merger extends Runnable, Serializable {
    class WindowInfo implements Serializable {
        public final long id, size;

        WindowInfo(long id, long size) {
            this.id = id;
            this.size = size;
        }
    }

    void populateTransientFields(StreamWindowManager windowManager);
}
