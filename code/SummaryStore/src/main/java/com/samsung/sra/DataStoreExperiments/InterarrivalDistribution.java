package com.samsung.sra.DataStoreExperiments;

import java.util.Random;

public interface InterarrivalDistribution {
    long next(Random random);
}
