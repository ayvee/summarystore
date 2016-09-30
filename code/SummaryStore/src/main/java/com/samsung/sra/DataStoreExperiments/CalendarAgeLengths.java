package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStoreExperiments.AgeLengthClass.Bin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CalendarAgeLengths {
    // Bins are: [0s, 1s), [1s, 1m), [1m, 1h), [1h, 1d), [1d, 1mo), [1mo, 1y), [1y, 10y), [10y, 100y)
    // In the code all times are in seconds

    private static final List<Bin> bins = Arrays.asList(
        new Bin("subsecond", 0, 0, 1), // the discrete set {0s}
        new Bin("seconds", 1, 59, 1), // the discrete set {1s, 2s, ..., 59s}
        new Bin("minutes", 1, 59, 60), // {1m, 2m, ..., 59m} = {60s, 120s, ..., 3540s}
        new Bin("hours", 1, 23, 3600), // {1h, 2h, ..., 23h} = {3600s, 7200s, ..., 3600 * 23 s}
        new Bin("days", 1, 6, 86400L), // {1d, 2d, ..., 6d} = {86400s, 2 * 86400s, ..., 6 * 86400s}
        new Bin("weeks", 1, 3, 7 * 86400L), // {1w, 2w, 3w} = {7 * 86400s, 14 * 86400s, 21 * 86400s}
        new Bin("months", 1, 11, 30 * 86400L), // {30 * 86400s, 60 * 86400s, ..., 330 * 86400s}
        // WARNING: ignoring leap years. Could do 3653/36525 in decades/centuries to mitigate partially
        new Bin("years", 1, 9, 365 * 86400L), // {365 * 86400s, 730 * 86400s, ..., 9 * 365 * 86400s}
        new Bin("decades", 1, 9, 3650 * 86400L), // {10 * 365 * 86400s, 20 * 365 * 86400s, ..., 90 * 365 * 86400s}
        new Bin("centuries", 1, 9, 36500 * 86400L) // {100 * 365 * 86400s, 200 * 365 * 86400s, ..., 900 * 365 * 86400s}
    );

    public static List<AgeLengthClass> getClasses(long maxAgeInSeconds) {
        return getClasses(maxAgeInSeconds, null);
    }

    public static List<AgeLengthClass> getClasses(long maxAgeInSeconds, String smallestBin) {
        List<Bin> legalBins;
        if (smallestBin == null) {
            legalBins = bins;
        } else {
            legalBins = new ArrayList<>();
            boolean foundStart = false;
            for (Bin bin: bins) {
                if (bin.name.equalsIgnoreCase(smallestBin)) {
                    foundStart = true;
                }
                if (foundStart) {
                    legalBins.add(bin);
                }
            }
        }

        List<AgeLengthClass> ret = new ArrayList<>();
        for (Bin ageBin: legalBins) {
            for (Bin lengthBin: legalBins) {
                if (ageBin.start + lengthBin.start - 1 <= maxAgeInSeconds) {
                    ret.add(new AgeLengthClass(ageBin, lengthBin, maxAgeInSeconds));
                }
            }
        }
        return ret;
    }
}
