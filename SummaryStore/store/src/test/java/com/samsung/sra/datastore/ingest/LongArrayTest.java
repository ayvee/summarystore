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

import com.samsung.sra.datastore.ingest.LongIngestBuffer.LongArray;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class LongArrayTest {
    @Test
    public void sanity() throws Exception {
        int N = 100;
        try (LongArray arr = new LongArray(N)) {
            for (int i = 0; i < N; ++i) {
                arr.put(i, N - i);
            }
            for (int i = 0; i < N; ++i) {
                assertEquals(N - i, arr.get(i));
            }
        }
    }
}