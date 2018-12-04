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

import com.samsung.sra.datastore.storage.StreamWindowManager;

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
