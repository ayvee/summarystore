package com.samsung.sra.Misc;

import com.clearspring.analytics.hash.MurmurHash;
import com.samsung.sra.DataStore.WindowAggrOperator;
import com.samsung.sra.DataStore.WindowAggrOperatorTypes;
import com.samsung.sra.DataStore.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * Created by n.agrawal1 on 5/27/16.
 */
public class HyperLogLogAggrOperator extends WindowAggrOperator<HyperLogLogAggrOperator, Double> implements ProbCounter {

    /** configuration for HLL estimation; these are applicable to all instances
     * of the HLL windows, and shouldn't be modified, hence static final.
     */

    private static final int hashSize = 32; // portion of the hash that is retained for estimation
    private static final int indexSize = 10; // 2^indexSize is the number of counting buckets
    private static final int regWidth = 4; // 2^regWidth - 1 is the bits considered for leading zero counting
    private static final int numRegisters = 1<<indexSize;
    private static final int leadingZeroBits = (1<<regWidth) - 1; // 15 for this config
    private static final int ignoreBits = hashSize - indexSize - leadingZeroBits; // number of msb that are ignored for leading zero counting

    private double estimate;
    private static int maxlZero;

    private int[] estimateBuckets;
    private static Logger logger = LoggerFactory.getLogger(HyperLogLogAggrOperator.class);

    public HyperLogLogAggrOperator(int opType) {
        //super(opType);
        maxlZero=0;
        estimate=0;
        estimateBuckets = new int[numRegisters];
        //System.out.println("Currently registered operators: " + WindowAggrOperator.returnAggrOperatorTypes().size());
    }

    protected static double getAlphaMM(final int p, final int m) {
        // See the paper.

        //logger.debug("alphamm for " + p + ":" + m +" = ...");
        switch (p) {
            case 4:
                return 0.673 * m * m;
            case 5:
                return 0.697 * m * m;
            case 6:
                return 0.709 * m * m;
            default:
                return (0.7213 / (1 + 1.079 / m)) * m * m;
        }
    }

    @Override
    public double getEstimate() {

        estimate=0;
        if(numRegisters!=estimateBuckets.length) {
            logger.error("Invalid values of  numRegisters and estimateBuckets.length");
        }

        // go through estimateBuckets and take harmonic mean
        for (int j=0; j<estimateBuckets.length; j++) {
            //estimate = alphamm * m^2 * sum(1/2^M[j]
            double tmp = 1<<estimateBuckets[j];
            estimate+= 1/tmp;
            //logger.debug("estimateBucket["+j+"] = " +  estimateBuckets[j] + ":" + tmp + ": estimate: " + estimate);
        }

        estimate=1/estimate;
        estimate*=getAlphaMM(indexSize, numRegisters);
        //logger.debug("getEstimate buckets: " + estimateBuckets.length + "; estimate: "+ estimate);

        return Math.ceil(estimate);
    }

    @Override
    public Double getError(long tStart, long tEnd) {
        return 0.0;
    }

    public HyperLogLogAggrOperator merge(HyperLogLogAggrOperator... hyperLogLogAggrOperators) {

        HyperLogLogAggrOperator mergedResult = new
                HyperLogLogAggrOperator(WindowAggrOperatorTypes.AggrOpTypes.HLL.ordinal());

        for (HyperLogLogAggrOperator o:hyperLogLogAggrOperators) {

            mergedResult.estimate+= o.estimate;

        }

        return mergedResult;

    }

    @Override
    public void insert(int value) {
        insertHashed(MurmurHash.hash(value));
    }

    public void insertHashed(int hash) {

        /** this hash is 32 bits; only use the lsb hashSize; then strip those bits into three parts:
        index (indexSize), bits used to count leading zeroes (leadingZeroBits), and ignore bits (ignoreBits)
         */

        /** 1. extract the hashSize relevant bits; drop 32-hashsize msb and then rightshift back */
        hash=(hash<<(32-hashSize))>>>(32-hashSize);

        /** 2. inspect indexSize bits */
        int indexBits = (hash<<(32-indexSize)>>>(32-indexSize));
        if(indexBits > 1<<indexSize) {
            logger.error("invalid indexSize and indexbits " + indexSize + ":" + indexBits);
        }

        /** 3. inspect leading zero bits and update estimateBuckets[indexBits]
         * for trial, lets use all of the 24 - 6 = 18 bits for leading zeroes
         * since hash is 32 int, 0x00ABCDEF, subtract "8" from every answer
         */

        int lZero = Integer.numberOfLeadingZeros(hash) - (32-hashSize) + 1; // add 1 as per Floujet
        maxlZero=maxlZero>lZero?maxlZero:lZero;
        estimateBuckets[indexBits]=estimateBuckets[indexBits]>lZero?estimateBuckets[indexBits]:lZero;
    }

    @Override
    public Double read() {
        return this.getEstimate();
    }

    @Override
    public ValueError<Double> readRange(long tStart, long tEnd) {

        ValueError<Double> ve = new ValueError<>();
        ve.value = this.getEstimate();
        ve.error = getError(tStart,tEnd);
        return ve;
    }

    public static void main(String[] args) {

        // Test the cardinality estimation
        int boundTrueCount = 1<<18; // test value 2^16
        int numInts = 1<<(Integer.SIZE*8);
        Random r = new Random();
        int trueCount = r.nextInt(boundTrueCount);
        HyperLogLogAggrOperator hll = new HyperLogLogAggrOperator(WindowAggrOperatorTypes.AggrOpTypes.HLL.ordinal());

        // insert a bunch of values
        for (int i=0; i<trueCount; i++) {
            hll.insert(r.nextInt());
        }

        // estimate the count
        System.out.println("True Count: "+trueCount+"; HLL Estimate: "+hll.getEstimate() + "; " +
                "maxLZero: " + maxlZero + "; LogLog estimate: " + Math.pow(2,maxlZero) +
                "; Error: " + String.format("%.2f", Math.abs(trueCount-hll.getEstimate())/trueCount * 100) + "%");
    }

}
