package com.samsung.sra.DataStore.Ingest;

import com.samsung.sra.DataStore.Storage.StreamWindowManager;
import com.samsung.sra.DataStore.SummaryWindow;
import com.samsung.sra.DataStore.Utilities;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;

/** Summarize values in full IngestBuffers into a sequence of SummaryWindows, and pass them on to Writer's queue */
class Summarizer implements Runnable, Serializable {
    static final IngestBuffer SHUTDOWN_SENTINEL = new IngestBuffer(0);
    static final IngestBuffer FLUSH_SENTINEL = new IngestBuffer(0);

    private final BlockingQueue<IngestBuffer> buffersToSummarize; // input queue
    private final BlockingQueue<SummaryWindow> windowsToWrite; // output queue
    private final BlockingQueue<IngestBuffer> emptyBuffers; // output queue (discards cleared out buffers here)
    private final CountBasedWBMH.FlushHandler flushHandler;

    private final int[] windowLengths;

    private long N = 0;

    private transient StreamWindowManager windowManager = null;

    Summarizer(int[] windowLengths, BlockingQueue<IngestBuffer> emptyBuffers,
               BlockingQueue<IngestBuffer> buffersToSummarize,
               BlockingQueue<SummaryWindow> windowsToWrite,
               CountBasedWBMH.FlushHandler flushHandler) {
        this.windowLengths = windowLengths;
        this.emptyBuffers = emptyBuffers;
        this.buffersToSummarize = buffersToSummarize;
        this.windowsToWrite = windowsToWrite;
        this.flushHandler = flushHandler;
    }

    void populateTransientFields(StreamWindowManager windowManager) {
        this.windowManager = windowManager;
    }

    @Override
    public void run() {
            while (true) {
                IngestBuffer buffer = Utilities.take(buffersToSummarize);
                if (buffer == SHUTDOWN_SENTINEL) {
                    Utilities.offerAndConfirm(windowsToWrite, Writer.SHUTDOWN_SENTINEL);
                    flushHandler.notifySummarizerFlushed();
                    break;
                } else if (buffer == FLUSH_SENTINEL) {
                    Utilities.offerAndConfirm(windowsToWrite, Writer.FLUSH_SENTINEL);
                    flushHandler.notifySummarizerFlushed();
                    continue;
                }
                int bs = 0, be; // index of first and last elements in the buffer belonging to current window
                for (int w = windowLengths.length - 1; w >= 0; --w) {
                    be = bs + windowLengths[w] - 1;
                    SummaryWindow window = windowManager.createEmptySummaryWindow(
                            buffer.getTimestamp(bs), buffer.getTimestamp(be), N + bs, N + be);
                    for (int c = bs; c <= be; ++c) {
                        windowManager.insertIntoSummaryWindow(window, buffer.getTimestamp(c), buffer.getValue(c));
                    }
                    Utilities.offerAndConfirm(windowsToWrite, window);
                    bs = be + 1;
                }
                assert bs == buffer.size();
                N += bs;

                buffer.clear();
                Utilities.offerAndConfirm(emptyBuffers, buffer);
            }

    }
}
