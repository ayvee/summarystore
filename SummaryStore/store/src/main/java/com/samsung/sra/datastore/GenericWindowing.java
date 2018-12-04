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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class GenericWindowing implements Windowing {
    private static final Logger logger = LoggerFactory.getLogger(GenericWindowing.class);
    private final WindowLengthsSequence windowLengths;

    public GenericWindowing(WindowLengthsSequence windowLengths) {
        this.windowLengths = windowLengths;
        addWindow((firstWindowLength = windowLengths.nextWindowLength()));
    }

    private final long firstWindowLength; // length of the first window (the one holding the newest element)

    // maps window length to the start marker of the first window of that length
    private final TreeMap<Long, Long> firstWindowOfLength = new TreeMap<>();
    // all window start markers in an ordered set
    private final TreeSet<Long> windowStartMarkers = new TreeSet<>();

    private long lastWindowStart = 0L, lastWindowLength = 0L;

    private void addWindow(long length) {
        assert length >= lastWindowLength && length > 0;
        lastWindowStart += lastWindowLength;
        if (length > lastWindowLength) firstWindowOfLength.put(length, lastWindowStart);
        windowStartMarkers.add(lastWindowStart);
        lastWindowLength = length;
    }

    /**
     * Add windows until we have one with length >= the specified target. Returns false
     * if the target length isn't achievable
     */
    private boolean addWindowsUntilLength(long targetLength) {
        if (targetLength > windowLengths.maxWindowSize()) {
            return false;
        } else {
            while (lastWindowLength < targetLength) {
                addWindow(windowLengths.nextWindowLength());
            }
            return true;
        }
    }

    /**
     * Add windows until we have at least one window marker larger than the target
     */
    private void addWindowsPastMarker(long targetMarker) {
        while (lastWindowStart <= targetMarker) {
            addWindow(windowLengths.nextWindowLength());
        }
    }

    /** Add windows until we have at least the specified count */
    private void addWindowsUntilCount(int count) {
        while (windowStartMarkers.size() < count) {
            addWindow(windowLengths.nextWindowLength());
        }
    }

    @Override
    public long getFirstContainingTime(long Tl, long Tr, long T) {
        assert 0 <= Tl && Tl <= Tr && Tr < T;
        long l = T-1 - Tr, r = T-1 - Tl, length = Tr - Tl + 1;

        if (!addWindowsUntilLength(length)) {
            return -1;
        }
        long firstMarker = firstWindowOfLength.ceilingEntry(length).getValue();
        if (firstMarker >= l) {
            /*logger.trace("getFirstContainingTime CASE 1: Tl = {}, Tr = {}, T = {}, [l, r] = [{}, {}], firstMarker = {}: retval = {}",
                    Tl, Tr, T, l, r, firstMarker, firstMarker + Tr + 1);*/
            // l' == firstMarker, where l' := N'-1 - Tr
            return firstMarker + Tr + 1;
        } else {
            // we've already hit the target window length, so [l, r] is either
            // already in the same window or will be once we move into the next window
            addWindowsPastMarker(l);
            long currWindowL = windowStartMarkers.floor(l), currWindowR = windowStartMarkers.higher(l) - 1;
            /*logger.trace("getFirstContainingTime CASE 2/3: Tl = {}, Tr = {}, T = {}, [l, r] = [{}, {}], firstMarker = {}, [currWindowL, currWindowR] = [{}, {}]",
                    Tl, Tr, T, l, r, firstMarker, currWindowL, currWindowR);*/
            if (r <= currWindowR) {
                // already in same window
                return T;
            } else {
                assert currWindowR - currWindowL + 1 >= length;
                // need to wait until next window, i.e. l' == currWindowR + 1, where l' := N'-1 - Tr
                return currWindowR + Tr + 2;
            }
        }
    }

    @Override
    public long getSizeOfFirstWindow() {
        return firstWindowLength;
    }

    @Override
    public List<Long> getWindowsCoveringUpto(long N) {
        if (N <= 0) return Collections.emptyList();
        addWindowsPastMarker(N);
        List<Long> ret = new ArrayList<>();
        long prevMarker = 0;
        for (long currMarker: windowStartMarkers) {
            if (currMarker == 0) continue; // first marker
            if (currMarker <= N) {
                ret.add(currMarker - prevMarker);
                // ret now covers the range [0, currMarker-1], of length currMarker <= N
                prevMarker = currMarker;
            } else {
                // adding another window would make ret cover the range [0, currMarker-1], of length currMarker > N
                break;
            }
        }
        return ret;
    }
}
