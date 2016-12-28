package com.samsung.sra.DataStoreExperiments;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class CDF implements Serializable {
    private final int nbins;

    private double min = Double.MAX_VALUE, max = Double.MIN_VALUE;
    // FYI: for some reason, the FST library broke when I tried storing (value, weight) in an ArrayList<Pair<Double>> instead
    private ArrayList<Double> values;
    private ArrayList<Double> valueWeights;
    private ArrayList<Double> cdf;
    private TreeMap<Double, Double> icdf;

    public CDF(int nbins) {
        assert nbins > 0;
        this.nbins = nbins;
        values = new ArrayList<>();
        valueWeights = new ArrayList<>();
        cdf = new ArrayList<>(nbins);
        icdf = new TreeMap<>();
    }

    /** mixture density */
    public CDF(int nbins, Collection<CDF> cdfs, Collection<Double> weights) {
        this(nbins);
        assert cdfs != null && weights != null && !weights.isEmpty() && weights.size() == cdfs.size();
        for (Iterator cdfi = cdfs.iterator(), weighti = weights.iterator(); cdfi.hasNext() && weighti.hasNext();) {
            CDF cdf = (CDF)cdfi.next();
            min = Math.min(min, cdf.min);
            max = Math.max(max, cdf.max);
            double groupWeight = (Double)weighti.next();
            for (int j = 0; j < cdf.values.size(); ++j) {
                values.add(cdf.values.get(j));
                valueWeights.add(groupWeight * cdf.valueWeights.get(j));
            }
        }
    }

    public synchronized void addValue(double obs, double weight) {
        clearCDF();
        min = Math.min(min, obs);
        max = Math.max(max, obs);
        values.add(obs);
        valueWeights.add(weight);
    }

    public synchronized void addValue(double obs) {
        addValue(obs, 1);
    }

    private int getBinNum(double x) {
        int b = (int)(nbins * (x - min) / (max - min));
        assert b >= 0 && b <= nbins;
        if (b == nbins) b = nbins - 1;
        return b;
    }

    private double getBinLeft(int b) {
        assert 0 <= b && b <= nbins;
        return min + (max - min) * b / nbins;
    }

    private double getBinRight(int b) {
        return getBinLeft(b+1);
    }

    /** P(X <= x) */
    public synchronized double getCumulativeDensity(double x) {
        //if (values.isEmpty()) return 0;
        if (x < min) return 0;
        if (x >= max) return 1;
        buildCDF();
        return cdf.get(getBinNum(x));
    }

    public synchronized double getQuantile(double Q) {
        //if (values.isEmpty()) return 0;
        if (Q < 0) return min;
        if (Q >= 1) return max;
        buildCDF();
        return (icdf.floorEntry(Q).getValue() + icdf.higherEntry(Q).getValue()) / 2;
    }

    private void writeCDF(Writer writer) throws IOException {
        writer.write("#x\tP(X <= x)\n");
        buildCDF();
        for (int b = 0; b < nbins; ++b) {
            writer.write(getBinRight(b) + "\t" + cdf.get(b) + "\n");
        }
    }

    public synchronized void writeCDF(String filename) throws IOException {
        try (BufferedWriter br = Files.newBufferedWriter(Paths.get(filename))) {
            writeCDF(br);
        }
    }

    private transient Writer stdout = null; // transient disables serializing this field
    public synchronized void printCDF() throws IOException {
        if (stdout == null) stdout = new OutputStreamWriter(System.out);
        writeCDF(stdout);
        stdout.flush();
    }

    private void buildCDF() {
        if (!cdf.isEmpty()) {
            return;
        }
        for (int b = 0; b < nbins; ++b) {
            cdf.add(0d);
        }

        // first construct unnormalized pdf, and compute the normalizing factor in parallel
        double normFactor = 0;
        for (int j = 0; j < values.size(); ++j) {
            int b = getBinNum(values.get(j));
            cdf.set(b, cdf.get(b) + valueWeights.get(j));
            normFactor += valueWeights.get(j);
        }
        // transform unnormalized pdf to normalized cdf
        cdf.set(0, cdf.get(0) / normFactor);
        for (int b = 1; b < nbins; ++b) {
            cdf.set(b, cdf.get(b-1) + cdf.get(b) / normFactor);
        }
        // also build inverse CDF
        icdf.put(0d, min);
        for (int b = 0; b < nbins; ++b) {
            icdf.put(cdf.get(b), getBinRight(b));
        }
        icdf.put(1d, max); // should be redundant with the last iteration of the previous for loop,
                           // but including to handle floating point rounding issues
    }

    private void clearCDF() {
        cdf.clear();
        icdf.clear();
    }

    public static void main(String[] args) throws Exception {
        CDF cdf = new CDF(10);
        cdf.addValue(0);
        cdf.addValue(0.05);
        cdf.addValue(0.05);
        cdf.addValue(0.05);
        cdf.addValue(0.05);
        cdf.addValue(0.06);
        cdf.addValue(0.09);
        cdf.addValue(0.1);
        cdf.addValue(0.85);
        cdf.addValue(1);
        cdf.printCDF();
        System.out.println();
        Arrays.asList(0.5, 0.95, 0.99, 0.99).forEach(Q ->
                System.out.println((Q * 100) + "th %ile = " + cdf.getQuantile(Q)));
    }
}
