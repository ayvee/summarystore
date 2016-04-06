package com.samsung.sra.DataStoreExperiments;

/**
 * Created by n.agrawal1 on 3/31/16.
 */
public class Pair<T> {

    private final T m_first;
    private final T m_second;

    public Pair(T first, T second) {
        m_first = first;
        m_second = second;
    }

    public T first() {
        return m_first;
    }

    public T second() {
        return m_second;
    }

}
