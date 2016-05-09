package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.ExponentialWindowLengths;
import com.samsung.sra.DataStore.PolynomialWindowLengths;
import com.samsung.sra.DataStore.WindowLengths;

public class PrintDecayFunction {
    public static void main(String[] args) {
        WindowLengths windowLengths;
        long N, W;
        try {
            if (args.length != 3) {
                throw new IllegalArgumentException("wrong argument count");
            }
            String decay = args[0];
            N = Long.parseLong(args[1]);
            W = Long.parseLong(args[2]);
            if (decay.equals("exponential")) {
                windowLengths = ExponentialWindowLengths.getWindowingOfSize(N, W);
            } else if (decay.startsWith("polynomial")) {
                int d = Integer.parseInt(decay.substring("polynomial".length()));
                windowLengths = PolynomialWindowLengths.getWindowingOfSize(d, N, W);
            } else {
                throw new IllegalArgumentException("unrecognized decay function");
            }
        } catch (IllegalArgumentException e) {
            System.err.println("SYNTAX ERROR: " + e.getMessage());
            System.err.println("\tPrintDecayFunction <decay_function_name> <stream_length> <num_windows>");
            System.exit(2);
            return;
        }
        System.out.println("#age\tnum_bytes");
        for (int n = 1; n <= N;) {
            long w = windowLengths.nextWindowLength();
            assert w > 0;
            double nBytes = 8d / w; // 8 bytes per element, divided by # elements in that window
            System.out.println(n + "\t" + nBytes);
            if (w > 1) {
                System.out.println((n+w-1) + "\t" + nBytes);
            }
            n += w;
        }
    }
}
