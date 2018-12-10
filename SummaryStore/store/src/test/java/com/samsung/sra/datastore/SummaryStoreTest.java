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

import com.samsung.sra.datastore.aggregates.CMSOperator;
import com.samsung.sra.datastore.aggregates.MaxOperator;
import com.samsung.sra.datastore.aggregates.SimpleCountOperator;
import com.samsung.sra.datastore.ingest.CountBasedWBMH;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Test;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class SummaryStoreTest {
    private static final long streamID = 0;

    @Test
    public void exponential() throws Exception {
        exponentialTest(true);
        exponentialTest(false);
    }

    private void exponentialTest(boolean withReadIndex) throws Exception {
        String storeLoc = "/tmp/tdstore";
        Runtime.getRuntime().exec(new String[]{"sh", "-c", "rm -rf " + storeLoc}).waitFor();

        // create and populate store
        SummaryStore store = new SummaryStore(storeLoc, new SummaryStore.StoreOptions().setKeepReadIndexes(withReadIndex));
        Windowing windowing = new GenericWindowing(new ExponentialWindowLengths(2));
        CountBasedWBMH wbmh = new CountBasedWBMH(windowing).setBufferSize(62);
        store.registerStream(streamID, wbmh,
                new SimpleCountOperator(),
                new CMSOperator(5, 100, 0),
                new MaxOperator());
        for (long i = 0; i < 1022; ++i) {
            if (i == 491) {
                store.startLandmark(streamID, i);
            }
            store.append(streamID, i, i % 10);
            if (i == 500) {
                store.endLandmark(streamID, i);
            }
        }
        store.flush(streamID);
        wbmh.setBufferSize(0);

        assertStateIsCorrect(store);

        // check unloading and loading works correctly
        store.unloadStream(streamID);
        boolean exceptionThrown = false;
        try {
            store.query(streamID, 0, 10, 2);
        } catch (StreamException e) {
            assertThat(e.getMessage(), containsString("unloaded"));
            exceptionThrown = true;
        }
        assertEquals(true, exceptionThrown);
        store.loadStream(streamID);
        assertStateIsCorrect(store);

        // close and reopen store (in read-only mode), then check everything still OK
        store.close();
        store = new SummaryStore(storeLoc, new SummaryStore.StoreOptions().setKeepReadIndexes(withReadIndex).setReadOnly(true));
        assertStateIsCorrect(store);

        store.close();
    }

    /*private void printState(SummaryStore store) throws Exception {
        store.printWindowState(streamID);
        long t0 = 1, t1 = 511;
        System.out.println("[" + t0 + ", " + t1 + "] count = " + store.query(streamID, t0, t1, 0, 0.95));
        System.out.println("[" + t0 + ", " + t1 + "] freq(8) = " + store.query(streamID, t0, t1, 1, 8L));
        System.out.println("[" + t0 + ", " + t1 + "] max = " + store.query(streamID, t0, t1, 2));
    }*/

    /*    OLD OUTPUT, with one by one partial buffer processing
    Stream 0 with 1022 elements in 14 summary windows and 1 landmark windows
        <summary-window: time range [0:495], count range [0:495], aggrs [491, com.clearspring.analytics.stream.frequency.CountMinSketch@ea30797, 9]>
        <summary-window: time range [496:743], count range [496:743], aggrs [243, com.clearspring.analytics.stream.frequency.CountMinSketch@7e774085, 9]>
        <summary-window: time range [744:867], count range [744:867], aggrs [124, com.clearspring.analytics.stream.frequency.CountMinSketch@3f8f9dd6, 9]>
        <summary-window: time range [868:929], count range [868:929], aggrs [62, com.clearspring.analytics.stream.frequency.CountMinSketch@aec6354, 9]>
        <summary-window: time range [930:960], count range [930:960], aggrs [31, com.clearspring.analytics.stream.frequency.CountMinSketch@1c655221, 9]>
        <summary-window: time range [961:976], count range [961:976], aggrs [16, com.clearspring.analytics.stream.frequency.CountMinSketch@58d25a40, 9]>
        <summary-window: time range [977:992], count range [977:992], aggrs [16, com.clearspring.analytics.stream.frequency.CountMinSketch@1b701da1, 9]>
        <summary-window: time range [993:1000], count range [993:1000], aggrs [8, com.clearspring.analytics.stream.frequency.CountMinSketch@726f3b58, 9]>
        <summary-window: time range [1001:1008], count range [1001:1008], aggrs [8, com.clearspring.analytics.stream.frequency.CountMinSketch@442d9b6e, 8]>
        <summary-window: time range [1009:1012], count range [1009:1012], aggrs [4, com.clearspring.analytics.stream.frequency.CountMinSketch@ee7d9f1, 9]>
        <summary-window: time range [1013:1016], count range [1013:1016], aggrs [4, com.clearspring.analytics.stream.frequency.CountMinSketch@15615099, 6]>
        <summary-window: time range [1017:1018], count range [1017:1018], aggrs [2, com.clearspring.analytics.stream.frequency.CountMinSketch@1edf1c96, 8]>
        <summary-window: time range [1019:1020], count range [1019:1020], aggrs [2, com.clearspring.analytics.stream.frequency.CountMinSketch@368102c8, 9]>
        <summary-window: time range [1021:1021], count range [1021:1021], aggrs [1, com.clearspring.analytics.stream.frequency.CountMinSketch@6996db8, 1]>
        <landmark-window: time range [491:500], 10 values
    [1, 511] count = <511.0, [511.0, 511.0]>
    [1, 511] freq(8) = <50.9866234190742, [1.0, 73001.0]>
    [1, 511] max = <9, false> */

    /* Stream 0 with 15 summary windows and 1 landmark windows
	    <summary-window: time range [0:495], count range [0:495], aggrs [491, com.clearspring.analytics.stream.frequency.CountMinSketch@35f983a6, 9]>
	    <summary-window: time range [496:743], count range [496:743], aggrs [243, com.clearspring.analytics.stream.frequency.CountMinSketch@7f690630, 9]>
	    <summary-window: time range [744:867], count range [744:867], aggrs [124, com.clearspring.analytics.stream.frequency.CountMinSketch@edf4efb, 9]>
	    <summary-window: time range [868:929], count range [868:929], aggrs [62, com.clearspring.analytics.stream.frequency.CountMinSketch@2f7a2457, 9]>
	    <summary-window: time range [930:960], count range [930:960], aggrs [31, com.clearspring.analytics.stream.frequency.CountMinSketch@566776ad, 9]>
	    <summary-window: time range [961:976], count range [961:976], aggrs [16, com.clearspring.analytics.stream.frequency.CountMinSketch@6108b2d7, 9]>
	    <summary-window: time range [977:991], count range [977:991], aggrs [15, com.clearspring.analytics.stream.frequency.CountMinSketch@1554909b, 9]>
	    <summary-window: time range [992:999], count range [992:999], aggrs [8, com.clearspring.analytics.stream.frequency.CountMinSketch@6bf256fa, 9]>
	    <summary-window: time range [1000:1007], count range [1000:1007], aggrs [8, com.clearspring.analytics.stream.frequency.CountMinSketch@6cd8737, 7]>
	    <summary-window: time range [1008:1011], count range [1008:1011], aggrs [4, com.clearspring.analytics.stream.frequency.CountMinSketch@22f71333, 9]>
	    <summary-window: time range [1012:1015], count range [1012:1015], aggrs [4, com.clearspring.analytics.stream.frequency.CountMinSketch@13969fbe, 5]>
	    <summary-window: time range [1016:1017], count range [1016:1017], aggrs [2, com.clearspring.analytics.stream.frequency.CountMinSketch@6aaa5eb0, 7]>
	    <summary-window: time range [1018:1019], count range [1018:1019], aggrs [2, com.clearspring.analytics.stream.frequency.CountMinSketch@3498ed, 9]>
	    <summary-window: time range [1020:1020], count range [1020:1020], aggrs [1, com.clearspring.analytics.stream.frequency.CountMinSketch@1a407d53, 0]>
	    <summary-window: time range [1021:1021], count range [1021:1021], aggrs [1, com.clearspring.analytics.stream.frequency.CountMinSketch@3d8c7aca, 1]>
    [1, 511] count = <511.0, [511.0, 511.0]>
    [1, 511] freq(8) = <50.9866234190742, [1.0, 74.0]>
    [1, 511] max = <9, false> */


    @SuppressWarnings("unchecked")
    private void assertStateIsCorrect(SummaryStore store) throws Exception {
        assertEquals(1022, store.getStreamStatistics(streamID).getNumValues());
        assertEquals(15, store.getNumSummaryWindows(streamID));
        assertEquals(1, store.getNumLandmarkWindows(streamID));

        // summary windows
        /* OLD, with one by one partial buffer processing
        Integer[]
                tss = {0, 496, 744, 868, 930, 961, 977, 993, 1001, 1009, 1013, 1017, 1019, 1021},
                tes = {495, 743, 867, 929, 960, 976, 992, 1000, 1008, 1012, 1016, 1018, 1020, 1021},
                css = {0, 496, 744, 868, 930, 961, 977, 993, 1001, 1009, 1013, 1017, 1019, 1021},
                ces = {495, 743, 867, 929, 960, 976, 992, 1000, 1008, 1012, 1016, 1018, 1020, 1021},
                //prevTSs = {-1, 0, 496, 744, 868, 930, 961, 977, 993, 1001, 1009, 1013, 1017, 1019},
                //nextTSs = {496, 744, 868, 930, 961, 977, 993, 1001, 1009, 1013, 1017, 1019, 1021, -1},
                op0 = {491, 243, 124, 62, 31, 16, 16, 8, 8, 4, 4, 2, 2, 1}, // count
                op2 = {9, 9, 9, 9, 9, 9, 9, 9, 8, 9, 6, 8, 9, 1}; // max*/
        Integer[]
                tss = {0, 496, 744, 868, 930, 961, 977, 992, 1000, 1008, 1012, 1016, 1018, 1020, 1021},
                tes = {495, 743, 867, 929, 960, 976, 991, 999, 1007, 1011, 1015, 1017, 1019, 1020, 1021},
                css = {0, 496, 744, 868, 930, 961, 977, 992, 1000, 1008, 1012, 1016, 1018, 1020, 1021},
                ces = {495, 743, 867, 929, 960, 976, 991, 999, 1007, 1011, 1015, 1017, 1019, 1020, 1021},
                op0 = {491, 243, 124, 62, 31, 16, 15, 8, 8, 4, 4, 2, 2, 1, 1}, // count
                op2 = {9, 9, 9, 9, 9, 9, 9, 9, 7, 9, 5, 7, 9, 0, 1}; // max
        List<SummaryWindow> summaryWindows = store.streams.get(streamID).windowManager
                .getSummaryWindowsOverlapping(0, 1022)
                .collect(Collectors.toList());
        assertSummaryPropertyEquals(tss, summaryWindows, w -> w.ts);
        assertSummaryPropertyEquals(tes, summaryWindows, w -> w.te);
        assertSummaryPropertyEquals(css, summaryWindows, w -> w.cs);
        assertSummaryPropertyEquals(ces, summaryWindows, w -> w.ce);
        //assertSummaryPropertyEquals(prevTSs, summaryWindows, w -> w.prevTS);
        //assertSummaryPropertyEquals(nextTSs, summaryWindows, w -> w.nextTS);
        assertSummaryPropertyEquals(op0, summaryWindows, w -> (long) w.aggregates[0]);
        assertSummaryPropertyEquals(op2, summaryWindows, w -> (long) w.aggregates[2]);

        // landmark window
        List<LandmarkWindow> landmarkWindows = store.streams.get(streamID).windowManager
                .getLandmarkWindowsOverlapping(0, 1022)
                .collect(Collectors.toList());
        assertEquals(1, landmarkWindows.size());
        LandmarkWindow landmarkWindow = landmarkWindows.get(0);
        assertEquals(491, landmarkWindow.ts);
        assertEquals(500, landmarkWindow.te);
        assertEquals(10, landmarkWindow.values.size());
        // maybe also check the actual values?

        // queries
        long t0 = 1, t1 = 511;
        double delta = 1e-6;
        ResultError<Double, ImmutablePair<Double, Double>> countRE = (ResultError) store.query(streamID, t0, t1, 0, 0.95);
        assertEquals(511, countRE.result, delta);
        assertEquals(511, countRE.error.getLeft(), delta);
        assertEquals(511, countRE.error.getRight(), delta);
        ResultError<Double, ImmutablePair<Double, Double>> freq8RE = (ResultError) store.query(streamID, t0, t1, 1, 8L);
        assertEquals(50.9866234190742, freq8RE.result, delta);
        assertEquals(1, freq8RE.error.getLeft(), delta);
        assertEquals(74, freq8RE.error.getRight(), delta);
        ResultError<Long, Boolean> maxRE = (ResultError) store.query(streamID, t0, t1, 2);
        assertEquals(new Long(9), maxRE.result);
        assertEquals(false, maxRE.error);
        ResultError<Double, ImmutablePair<Double, Double>> fullCountRE = (ResultError) store.query(streamID, 0, 1021, 0, 0.95);
        assertEquals(1022, fullCountRE.result, delta);
        assertEquals(1022, fullCountRE.error.getLeft(), delta);
        assertEquals(1022, fullCountRE.error.getRight(), delta);
    }

    private static void assertSummaryPropertyEquals(Integer[] expected, List<SummaryWindow> windows,
                                                    Function<SummaryWindow, Long> getter) {
        assertArrayEquals(expected, windows.stream()
                .map(getter)
                .map(Number::intValue)
                .collect(Collectors.toList())
                .toArray(new Integer[0]));
    }
}
