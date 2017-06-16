package com.samsung.sra.DataStore.Ingest;

import com.samsung.sra.DataStore.Storage.StreamWindowManager;

import java.io.Serializable;

/**
 * Implements WBMH. Will be given a BlockingQueue<WindowInfo> in constructor which it is expected to process in run()
 */
abstract class Merger implements Runnable, Serializable {
    static final Merger.WindowInfo SHUTDOWN_SENTINEL = new Merger.WindowInfo(-1L, -1L);
    static final Merger.WindowInfo FLUSH_SENTINEL = new Merger.WindowInfo(-1L, -1L);

    static class WindowInfo implements Serializable {
        public final long id, size;

        WindowInfo(long id, long size) {
            this.id = id;
            this.size = size;
        }
    }

    abstract void populateTransientFields(StreamWindowManager windowManager);
}
