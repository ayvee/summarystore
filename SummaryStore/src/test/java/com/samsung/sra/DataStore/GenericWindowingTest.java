package com.samsung.sra.DataStore;

import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class GenericWindowingTest {
    @Test
    public void getFirstContainingTime() throws Exception {
        GenericWindowing exp = new GenericWindowing(new ExponentialWindowLengths(2));
        assertEquals(2 + 1, exp.getFirstContainingTime(0, 1, 2));
        assertEquals(4 + 3, exp.getFirstContainingTime(0, 3, 4));
        assertEquals(8 + 7, exp.getFirstContainingTime(0, 7, 8));
    }

    @Test
    public void getSizeOfFirstWindow() throws Exception {
        GenericWindowing exp = new GenericWindowing(new ExponentialWindowLengths(2));
        assertEquals(1, exp.getSizeOfFirstWindow(), 1);

        final long S = 2492;
        GenericWindowing rp = new GenericWindowing(new WindowLengthsSequence() {
            private long i = 1;

            @Override
            public long nextWindowLength() {
                return i++ * S;
            }
        });
        assertEquals(S, rp.getSizeOfFirstWindow());
    }

    @Test
    public void getWindowsCoveringUpto() throws Exception {
        GenericWindowing exp = new GenericWindowing(new ExponentialWindowLengths(2));
        assertThat(exp.getWindowsCoveringUpto(62), is(Arrays.asList(1L, 2L, 4L, 8L, 16L)));
        assertThat(exp.getWindowsCoveringUpto(63), is(Arrays.asList(1L, 2L, 4L, 8L, 16L, 32L)));
    }
}