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

import java.io.Serializable;
import java.util.List;

/**
 * These are the three properties of a window sequence that WBMH actually needs to know.
 * The GenericWindowing implementor can build a Windowing out of any arbitrary
 * WindowLengthsSequence, but it can be very time/space inefficient for e.g. gradual
 * decay functions, and specialized implementations can potentially be much faster
 */
public interface Windowing extends Serializable {
    /**
     * Return the first T' >= T such that at time T' the interval [l, r] is contained
     * inside a single window. l and r are absolute timestamps (not ages)
     */
    long getFirstContainingTime(long l, long r, long T);

    // Strictly, unnecessary; can replace with getFirstContainingTime(T-k, T, T+1) == 0
    long getSizeOfFirstWindow();

    /**
     * Return the sizes of the first K windows, where
     *    first K windows cover <= N elements
     *    first K+1 windows cover > N elements
     *
     * (Used to calculate ingest buffer shape.)
     */
    List<Long> getWindowsCoveringUpto(long N);
}
