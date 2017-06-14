package com.samsung.sra.DataStore.Ingest;

import com.samsung.sra.DataStore.Utilities;
import org.apache.commons.math3.util.Pair;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;

/** Ingest values into initially empty buffers, and once buffers fill up move them to Summarizer's queue */
class Ingester implements Serializable {
    private final BlockingQueue<IngestBuffer> emptyBuffers; // input queue
    private final BlockingQueue<IngestBuffer> buffersToSummarize; // output queue

    // WARNING: should be declared volatile if external user code appends to the same stream from more than one thread
    private IngestBuffer activeBuffer = null;

    Ingester(BlockingQueue<IngestBuffer> emptyBuffers, BlockingQueue<IngestBuffer> buffersToSummarize) {
        this.emptyBuffers = emptyBuffers;
        this.buffersToSummarize = buffersToSummarize;
    }

    /** NOTE: must externally serialize all append() and close() */
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
            Utilities.put(buffersToSummarize, activeBuffer);
            activeBuffer = null;
        }
    }

    /**
     * Initiate flush/shutdown of summarizer and parser, and if we have a partially filled buffer return it (as well
     * as the list of empty buffers) so that the caller can process its contents using appendUnbuffered.
     *
     * NOTE: must externally serialize all append() and close() */
    Pair<IngestBuffer, BlockingQueue<IngestBuffer>> flush(boolean shutdown) {
        Pair<IngestBuffer, BlockingQueue<IngestBuffer>> ret;
        if (activeBuffer != null && activeBuffer.size() > 0) {
            assert !activeBuffer.isFull();
            ret = new Pair<>(activeBuffer, emptyBuffers);
            activeBuffer = null;
        } else {
            ret = new Pair<>(null, null);
        }
        // initiate graceful summarizer and writer shutdown
        Utilities.put(buffersToSummarize, shutdown ? Summarizer.SHUTDOWN_SENTINEL : Summarizer.FLUSH_SENTINEL);
        return ret;
    }
}
