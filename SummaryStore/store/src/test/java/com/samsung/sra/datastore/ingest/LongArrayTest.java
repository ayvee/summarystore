package com.samsung.sra.datastore.ingest;

import com.samsung.sra.datastore.ingest.LongIngestBuffer.LongArray;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class LongArrayTest {
    @Test
    public void sanity() throws Exception {
        int N = 100;
        try (LongArray arr = new LongArray(N)) {
            for (int i = 0; i < N; ++i) {
                arr.put(i, N - i);
            }
            for (int i = 0; i < N; ++i) {
                assertEquals(N - i, arr.get(i));
            }
        }
    }
}