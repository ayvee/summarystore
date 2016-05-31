package com.samsung.sra.DataStore;

/**
 * Created by n.agrawal1 on 5/20/16.
 */
public interface ProbCounter {
    double getEstimate();
    void insert(int value);
}
