/*
* Copyright 2016 Samsung Research America. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.samsung.sra.datastore.ingest;

import com.samsung.sra.datastore.storage.BackingStoreException;
import com.samsung.sra.datastore.storage.StreamWindowManager;
import com.samsung.sra.datastore.SummaryWindow;
import com.samsung.sra.datastore.Utilities;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;

/** Write SummaryWindows to backing store and notify merger */
class Writer implements Runnable, Serializable {
    static final SummaryWindow SHUTDOWN_SENTINEL = new SummaryWindow();
    static final SummaryWindow FLUSH_SENTINEL = new SummaryWindow();

    private final BlockingQueue<SummaryWindow> windowsToWrite; // input queue
    private final BlockingQueue<Merger.WindowInfo> newWindowNotifications; // output queue, feeding into Merger
    private final CountBasedWBMH.FlushBarrier flushBarrier;

    private transient StreamWindowManager windowManager;

    Writer(BlockingQueue<SummaryWindow> windowsToWrite, BlockingQueue<Merger.WindowInfo> newWindowNotifications,
           CountBasedWBMH.FlushBarrier flushBarrier) {
        this.windowsToWrite = windowsToWrite;
        this.newWindowNotifications = newWindowNotifications;
        this.flushBarrier = flushBarrier;
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
                    flushBarrier.notify(CountBasedWBMH.FlushBarrier.WRITER);
                    break;
                } else if (window == FLUSH_SENTINEL) {
                    flushBarrier.notify(CountBasedWBMH.FlushBarrier.WRITER);
                    continue;
                }
                windowManager.putSummaryWindow(window);
                Utilities.put(newWindowNotifications, new Merger.WindowInfo(window.ts, window.ce - window.cs + 1));
            }
        } catch (BackingStoreException e) {
            throw new RuntimeException(e);
        }
    }
}
