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
package com.samsung.sra.datastore;

import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class GenericWindowingTest {
    @Test
    public void getFirstContainingTime() throws Exception {
        GenericWindowing exp = new GenericWindowing(new ExponentialWindowLengths(2));
        assertEquals(101, exp.getFirstContainingTime(98, 99, 100));
        assertEquals(103, exp.getFirstContainingTime(96, 99, 100));
        assertEquals(107, exp.getFirstContainingTime(92, 99, 100));
        assertEquals(115, exp.getFirstContainingTime(84, 99, 100));
    }

    @Test
    public void getSizeOfFirstWindow() throws Exception {
        GenericWindowing exp = new GenericWindowing(new ExponentialWindowLengths(2));
        assertEquals(1, exp.getSizeOfFirstWindow(), 1);

        final long S = 2492;
        GenericWindowing rp = new GenericWindowing(new WindowLengthsSequence() {
            private long i = 1;

            @Override
            public long nextWindowLength() {
                return i++ * S;
            }
        });
        assertEquals(S, rp.getSizeOfFirstWindow());
    }

    @Test
    public void getWindowsCoveringUpto() throws Exception {
        GenericWindowing exp = new GenericWindowing(new ExponentialWindowLengths(2));
        assertThat(exp.getWindowsCoveringUpto(62), is(Arrays.asList(1L, 2L, 4L, 8L, 16L)));
        assertThat(exp.getWindowsCoveringUpto(63), is(Arrays.asList(1L, 2L, 4L, 8L, 16L, 32L)));
    }
}