package com.samsung.sra.DataStoreExperiments;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

/** Convert a (timestamp, value) CSV file into a binary-encoded file suitable for BinStreamGenerator */
public class CSV2Bin {
    private static final Logger logger = LoggerFactory.getLogger(CSV2Bin.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("SYNTAX: CSV2Bin <infile.tsv> <outfile.bin>");
            System.exit(2);
        }
        int N = 0;
        try (BufferedReader br = Files.newBufferedReader(Paths.get(args[0]))) {
            while (br.readLine() != null) {
                assert N != Integer.MAX_VALUE;
                ++N;
            }
        }
        long[] ts = new long[N], vs = new long[N];
        try (BufferedReader br = Files.newBufferedReader(Paths.get(args[0]))) {
            String line;
            int n = 0;
            while ((line = br.readLine()) != null) {
                if (n % 10_000_000 == 0) {
                    logger.info("processing line {}", String.format("%,d", n));
                }
                int i = line.indexOf(','), l = line.length();
                ts[n] = Long.parseLong(line.substring(0, i));
                vs[n] = Long.parseLong(line.substring(i + 1, l));
                n++;
            }
        }
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(args[1]))) {
            oos.writeObject(ts);
            oos.writeObject(vs);
        }
        logger.info("Done");
    }
}
