package com.samsung.sra.experiments;

import java.util.SplittableRandom;

public interface Distribution<T> {
    /* Implementors must define a constructor with signature Distribution(Toml params). It will be called via reflection. */

    T next(SplittableRandom random);
}
