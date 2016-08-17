package com.samsung.sra.DataStoreExperiments;

import java.util.Random;

public interface Distribution<T> {
    T next(Random random);
}
