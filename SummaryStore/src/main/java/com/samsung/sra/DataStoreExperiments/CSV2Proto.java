package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.protocol.TimeSeriesOuterClass.TimeSeries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

/** Convert a (timestamp, value) CSV file into a binary proto-encoded file suitable for ProtoStreamGenerator */
public class CSV2Proto {
    private static final Logger logger = LoggerFactory.getLogger(CSV2Proto.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("SYNTAX: CSV2Proto <infile.tsv> <outfile.pbin>");
            System.exit(2);
        }
        TimeSeries.Builder builder = TimeSeries.newBuilder();
        try (BufferedReader br = Files.newBufferedReader(Paths.get(args[0]))) {
            long N = 0;
            String line;
            while ((line = br.readLine()) != null) {
                if (N % 10_000_000 == 0) {
                    logger.info("processing line {}", String.format("%,d", N));
                }
                int i = line.indexOf(','), l = line.length();
                builder.addTimestamp(Long.parseLong(line.substring(0, i)));
                builder.addValue(Long.parseLong(line.substring(i + 1, l)));
                ++N;
            }
        }
        TimeSeries series = builder.build();
        logger.info("Built proto, writing to disk");
        try (FileOutputStream fos = new FileOutputStream(args[1])) {
            series.writeTo(fos);
        }
        logger.info("Done");
    }
}
