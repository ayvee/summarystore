package com.samsung.sra.DataStore.Ingest;

import com.samsung.sra.DataStore.Storage.BackingStoreException;
import com.samsung.sra.DataStore.Storage.StreamWindowManager;
import com.samsung.sra.DataStore.SummaryWindow;
import com.samsung.sra.DataStore.Utilities;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;

/** Write SummaryWindows to backing store and notify merger */
class Writer implements Runnable, Serializable {
    static final SummaryWindow SHUTDOWN_SENTINEL = new SummaryWindow();
    static final SummaryWindow FLUSH_SENTINEL = new SummaryWindow();

    private final BlockingQueue<SummaryWindow> windowsToWrite; // input queue
    private final BlockingQueue<Merger.WindowInfo> newWindowNotifications; // output queue, feeding into Merger
    private final CountBasedWBMH.FlushHandler flushHandler;

    private transient StreamWindowManager windowManager;

    Writer(BlockingQueue<SummaryWindow> windowsToWrite, BlockingQueue<Merger.WindowInfo> newWindowNotifications,
           CountBasedWBMH.FlushHandler flushHandler) {
        this.windowsToWrite = windowsToWrite;
        this.newWindowNotifications = newWindowNotifications;
        this.flushHandler = flushHandler;
    }

    void populateTransientFields(StreamWindowManager windowManager) {
        this.windowManager = windowManager;
    }

    @Override
    public void run() {
        try {
            while (true) {
                SummaryWindow window = Utilities.take(windowsToWrite);
                if (window == SHUTDOWN_SENTINEL) {
                    flushHandler.notifyWriterFlushed();
                    break;
                } else if (window == FLUSH_SENTINEL) {
                    flushHandler.notifyWriterFlushed();
                    continue;
                }
                windowManager.putSummaryWindow(window);
                Utilities.offerAndConfirm(newWindowNotifications, new Merger.WindowInfo(window.ts, window.ce - window.cs + 1));
            }
        } catch (BackingStoreException e) {
            throw new RuntimeException(e);
        }
    }
}
