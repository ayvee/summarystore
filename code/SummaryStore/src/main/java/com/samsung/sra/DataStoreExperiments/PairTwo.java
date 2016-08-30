package com.samsung.sra.DataStoreExperiments;

import java.io.Serializable;

/**
 * Created by n.agrawal1 on 3/31/16.
 */
public class PairTwo<T1, T2> implements Serializable {
    private final T1 m_first;
    private final T2 m_second;

    public PairTwo(T1 first, T2 second) {
        m_first = first;
        m_second = second;
    }

    public T1 first() {
        return m_first;
    }

    public T2 second() {
        return m_second;
    }
}
