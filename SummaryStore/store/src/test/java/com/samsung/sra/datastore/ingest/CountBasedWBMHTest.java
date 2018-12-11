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

import com.samsung.sra.datastore.ExponentialWindowLengths;
import com.samsung.sra.datastore.GenericWindowing;
import com.samsung.sra.datastore.WindowOperator;
import com.samsung.sra.datastore.aggregates.SimpleCountOperator;
import com.samsung.sra.datastore.storage.MainMemoryBackingStore;
import com.samsung.sra.datastore.storage.StreamWindowManager;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class CountBasedWBMHTest {
    @Test
    public void exponential() throws Exception {
        StreamWindowManager swm = new StreamWindowManager(0L, new WindowOperator[]{new SimpleCountOperator()}, true);
        swm.populateTransientFields(new MainMemoryBackingStore());
        CountBasedWBMH wbmh = new CountBasedWBMH(new GenericWindowing(new ExponentialWindowLengths(2)));
        wbmh.populateTransientFields(swm);

        Integer[][] expectedEvolution = {
                {1},
                {1, 1},
                {2, 1},
                {2, 1, 1},
                {2, 2, 1},
                {2, 2, 1, 1},
                {4, 2, 1},
                {4, 2, 1, 1},
                {4, 2, 2, 1},
                {4, 2, 2, 1, 1},
                {4, 4, 2, 1},
                {4, 4, 2, 1, 1},
                {4, 4, 2, 2, 1},
                {4, 4, 2, 2, 1, 1},
                {8, 4, 2, 1}
        };

        for (int t = 0; t < expectedEvolution.length; ++t) {
            wbmh.append(t, 0L);
            wbmh.flush();
            //.map(w -> (int) (w.ce - w.cs + 1))
            assertArrayEquals(expectedEvolution[t], swm
                    .getSummaryWindowsOverlapping(0, t)
                    .map(w -> ((Number) w.aggregates[0]).intValue())
                    .toArray(Integer[]::new));
        }
    }
}
