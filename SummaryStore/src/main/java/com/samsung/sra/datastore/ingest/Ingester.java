package com.samsung.sra.datastore.ingest;

import com.samsung.sra.datastore.Utilities;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;

/** Ingest values into initially empty buffers, and once buffers fill up move them to Summarizer's queue */
class Ingester implements Serializable {
    private final BlockingQueue<IngestBuffer> emptyBuffers; // input queue
    private final BlockingQueue<IngestBuffer> summarizerQueue; // output queue

    // WARNING: should be declared volatile if external user code appends to the same stream from more than one thread
    private IngestBuffer activeBuffer = null;

    Ingester(BlockingQueue<IngestBuffer> emptyBuffers, BlockingQueue<IngestBuffer> summarizerQueue) {
        this.emptyBuffers = emptyBuffers;
        this.summarizerQueue = summarizerQueue;
    }

    /** NOTE: must externally serialize all append() and flush() */
    void append(long ts, Object value) {
        while (activeBuffer == null) {
            try {
                activeBuffer = emptyBuffers.take();
            } catch (InterruptedException ignored) {
            }
        }
        assert !activeBuffer.isFull();
        activeBuffer.append(ts, value);
        if (activeBuffer.isFull()) {
            Utilities.put(summarizerQueue, activeBuffer);
            activeBuffer = null;
        }
    }

    /**
     * Send any outstanding values to summarizer (whether buffer is full or not) and initiate summarizer flush.
     *
     * NOTE: must externally serialize all append() and flush() */
    void flush(boolean shutdown) {
        if (activeBuffer != null && activeBuffer.size() > 0) {
            assert !activeBuffer.isFull();
            Utilities.put(summarizerQueue, activeBuffer);
            activeBuffer = null;
        }
        // initiate graceful summarizer shutdown
        Utilities.put(summarizerQueue, shutdown ? Summarizer.SHUTDOWN_SENTINEL : Summarizer.FLUSH_SENTINEL);
    }
}
