package com.samsung.sra.DataStore;

import java.util.*;

public class GenericWindowing implements Windowing {
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
            // l' == firstMarker, where l' := N'-1 - Tr
            return firstMarker + Tr + 1;
        } else {
            // we've already hit the target window length, so [l, r] is either
            // already in the same window or will be once we move into the next window
            addWindowsPastMarker(l);
            long currWindowL = windowStartMarkers.floor(l), currWindowR = windowStartMarkers.higher(l) - 1;
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
    public List<Long> getSizeOfFirstKWindows(int k) {
        addWindowsUntilCount(k);
        List<Long> ret = new ArrayList<>();
        long prevMarker = -1;
        for (long currMarker: windowStartMarkers) {
            if (prevMarker != -1) {
                ret.add(currMarker - prevMarker);
                if (ret.size() == k) break;
            }
            prevMarker = currMarker;
        }
        if (ret.size() == k - 1) {
            assert prevMarker == lastWindowStart;
            ret.add(lastWindowLength);
        } else {
            assert ret.size() == k;
        }
        return ret;
    }
}
