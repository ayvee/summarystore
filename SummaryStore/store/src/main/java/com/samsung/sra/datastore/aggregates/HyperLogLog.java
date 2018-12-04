/*
* Copyright 2016 Samsung Research America. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.samsung.sra.datastore.aggregates;

import com.clearspring.analytics.hash.MurmurHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class HyperLogLog {

    /*

    This implementation is based on the paper http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
    HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm
    Authors: Philippe Flajolet, Éric Fusy, Olivier Gandouet, Frédéric Meunier
    */



    /** configuration for HLL estimation; these are applicable to all instances
     * of the HLL windows, and shouldn't be modified, hence static final.
     */

    private static final int hashSize = 32; // portion of the hash that is retained for estimation
    private static final int indexSize = 10; // 2^indexSize is the number of counting buckets
    private static final int regWidth = 4; // 2^regWidth - 1 is the bits considered for leading zero counting
    private static final int numRegisters = 1<<indexSize;
    private static final int leadingZeroBits = (1<<regWidth) - 1; // 15 for this config
    private static final int ignoreBits = hashSize - indexSize - leadingZeroBits; // number of msb that are ignored for leading zero counting

    double estimate;
    private static int maxlZero;

    private int[] estimateBuckets;
    private static Logger logger = LoggerFactory.getLogger(HyperLogLog.class);

    public HyperLogLog() {
        //super(opType);
        maxlZero=0;
        estimate=0;
        estimateBuckets = new int[numRegisters];
        //System.out.println("Currently registered operators: " + WindowOperator.returnAggrOperatorTypes().size());
    }

    protected static double computeAlphaMsquared(final int p, final int m) {
        // refer to http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
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
        estimate*= computeAlphaMsquared(indexSize, numRegisters);
        //logger.debug("getEstimate buckets: " + estimateBuckets.length + "; estimate: "+ estimate);

        return Math.ceil(estimate);
    }

    public HyperLogLog merge(HyperLogLog... hyperLogLogAggrOperators) {
        HyperLogLog mergedResult = new HyperLogLog();

        for (HyperLogLog o:hyperLogLogAggrOperators) {
            mergedResult.estimate+= o.estimate;
        }

        return mergedResult;
    }

    public void insert(Integer value) {
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

    public static void main(String[] args) {

        // Test the cardinality estimation
        int boundTrueCount = 1<<18; // test value 2^16
        int numInts = 1<<(Integer.SIZE*8);
        Random r = new Random();
        int trueCount = r.nextInt(boundTrueCount);
        HyperLogLog hll = new HyperLogLog();

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
