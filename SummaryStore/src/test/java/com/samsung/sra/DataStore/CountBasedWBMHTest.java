package com.samsung.sra.DataStore;

import com.samsung.sra.DataStore.Aggregates.SimpleCountOperator;
import com.samsung.sra.DataStore.Storage.PeekableBackingStore;
import com.samsung.sra.DataStore.Storage.StreamWindowManager;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class CountBasedWBMHTest {
    @Test
    public void exponential() throws Exception {
        long streamID = 0L;
        PeekableBackingStore store = new PeekableBackingStore();
        WindowOperator[] operators = {new SimpleCountOperator()};
        StreamWindowManager swm = new StreamWindowManager(streamID, operators);
        swm.populateTransientFields(store);
        CountBasedWBMH wbmh = new CountBasedWBMH(new GenericWindowing(new ExponentialWindowLengths(2)), 0);

        Object[] v = {0L};

        wbmh.append(swm, 0, v);
        assertWindowLengthsEqual(store, streamID, 1);
        wbmh.append(swm, 1, v);
        assertWindowLengthsEqual(store, streamID, 1, 1);
        wbmh.append(swm, 2, v);
        assertWindowLengthsEqual(store, streamID, 1, 2);
        wbmh.append(swm, 3, v);
        assertWindowLengthsEqual(store, streamID, 1, 1, 2);
        wbmh.append(swm, 4, v);
        assertWindowLengthsEqual(store, streamID, 1, 2, 2);
        wbmh.append(swm, 5, v);
        assertWindowLengthsEqual(store, streamID, 1, 1, 2, 2);
        wbmh.append(swm, 6, v);
        assertWindowLengthsEqual(store, streamID, 1, 2, 4);
        wbmh.append(swm, 7, v);
        assertWindowLengthsEqual(store, streamID, 1, 1, 2, 4);
        wbmh.append(swm, 8, v);
        assertWindowLengthsEqual(store, streamID, 1, 2, 2, 4);
        wbmh.append(swm, 9, v);
        assertWindowLengthsEqual(store, streamID, 1, 1, 2, 2, 4);
        wbmh.append(swm, 10, v);
        assertWindowLengthsEqual(store, streamID, 1, 2, 4, 4);
        wbmh.append(swm, 11, v);
        assertWindowLengthsEqual(store, streamID, 1, 1, 2, 4, 4);
        wbmh.append(swm, 12, v);
        assertWindowLengthsEqual(store, streamID, 1, 2, 2, 4, 4);
        wbmh.append(swm, 13, v);
        assertWindowLengthsEqual(store, streamID, 1, 1, 2, 2, 4, 4);
        wbmh.append(swm, 14, v);
        assertWindowLengthsEqual(store, streamID, 1, 2, 4, 8);
    }

    private static void assertWindowLengthsEqual(PeekableBackingStore store, long streamID, long... lengths) {
        assertTrue(store.summaryWindows.containsKey(streamID));
        Collection<SummaryWindow> windows = store.summaryWindows.get(streamID).values();
        long[] windowCounts = new long[windows.size()];
        int i = 0;
        for (SummaryWindow window: windows) {
            windowCounts[i++] = window.cEnd - window.cStart + 1;
        }
        Arrays.sort(windowCounts);
        assertArrayEquals(lengths, windowCounts);
    }
}
