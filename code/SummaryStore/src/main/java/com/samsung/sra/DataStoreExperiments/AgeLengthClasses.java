package com.samsung.sra.DataStoreExperiments;

import com.samsung.sra.DataStore.*;
import javassist.bytecode.annotation.DoubleMemberValue;
import org.rocksdb.RocksDBException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.io.*;
import java.util.Vector;

import static org.apache.commons.math3.util.FastMath.floor;
import static org.apache.commons.math3.util.FastMath.log;
import static org.apache.commons.math3.util.FastMath.pow;

/**
 * Created by n.agrawal1 on 3/30/16.
 */
public class AgeLengthClasses {

    //public double[] pAgeLengthType;
    public static Random rClass, rQuery;
    Vector<Double> pValues;
    public double pSum;
    public double[] testAgeBuckets, testLengthBuckets;

    public enum Age {
        
        // in increasing order: A_1 newest, A_n oldest
        A1(0), A2(1), A3(2);
        
        private int Code;

        private Age(int Code) {
            this.Code=Code;
        }

        public int getCode() {
            return this.Code;
        }

        public void printCodes() {
            for (Age value:Age.values())
                System.out.println("Name: " + value.name());
        }

        public static int size = Age.values().length;
    }

    public enum Length {

        // in increasing order: L_1 shortest, A_n longest
        L1(0), L2(1), L3(2), L4(3), L5(4), L6(5), L7(6), L8(7), L9(8), L10(9);

        private int Code;

        private Length(int Code) {
            this.Code=Code;
        }

        public int getCode() {
            return this.Code;
        }

        public void printCodes() {
            for (Length value:Length.values())
                System.out.println("Name: " + value.name());
        }

        public static int size = Length.values().length;
    }

    AgeLengthClasses() throws AssertionError {


        //pAgeLength = new double[Age.size][Length.size];
        pValues = new Vector<Double>();
        double pSum = 0;
        rClass = new Random();
        rQuery = new Random();

    }

    AgeLengthClasses(Vector pValues) throws AssertionError {
        Iterator it = pValues.iterator();
        double normpSum = 0d;
        pSum = 0;
        while(it.hasNext())
            pSum += (double) it.next();
        System.out.println("pSum: " + pSum);
        rClass = new Random();
        rQuery = new Random();
        this.pValues = new Vector<Double>();
        //normalize
        it = pValues.iterator();
        while(it.hasNext()) {
            this.pValues.add((double) it.next() / pSum);
        }

        it = this.pValues.iterator();
        while(it.hasNext()) {
            normpSum+=(double) it.next();
        }
        System.out.println("Normalized pValues: " + this.pValues.toString()  + " Normalized Sum: " + normpSum);
    }

    public int selectRandomAgeLengthClass() {

        Iterator it = this.pValues.iterator();
        double randpSum = 0L;
        int i=0;

        while(it.hasNext() && randpSum < (double) rClass.nextFloat()) {
            randpSum += (double) it.next();
            i++;
        }
        return i-1;
    }

    public void createTestBuckets(StreamID testSID) throws StreamException{

        /* Eventually this will be replaced by the interface calls
           in SummaryStore

            long sAge = getStreamAgeInSeconds(StreamID streamID);
            long sLength = getStreamLength(StreamID streamID);

            testSID currently not used
        */

        double sAge = 42949672960L;
        double sLength = 42949672960L;
        double ageBSize, lengthBSize;
        testAgeBuckets = new double[Age.size];
        testLengthBuckets = new double[Length.size];

        /* The following code for creating test age and length buckets should be moved to a separate
            function; can be created as a HashMap(StreamID -> TestBuckets) for repeated experiments
         */

        if (sAge>0 && sLength >0) {

            // Query age is the stream age divided into roughly equal bins of size sAge/#age categories
            ageBSize = (log(2,sAge)/Age.size); // remainder: log(sAge)%Age.size
            System.out.println("Age Bucket Size: " + ageBSize + "  Age.size: " + Age.size);
            for(int i=0; i<Age.size-1; i++) {
                testAgeBuckets[i] = floor(pow(2, ageBSize * (i + 1)));
                System.out.println("Age Bucket " + i + ": " + testAgeBuckets[i]);
            }
            testAgeBuckets[Age.size-1]=sAge; // last bucket gets leftover; potentially larger than other windows
            System.out.println("Age Bucket " + (Age.size-1) + ": " + testAgeBuckets[Age.size-1]);

            /** Query length can be much smaller than the stream length
             * 0.1% for 1M stream = 1000, 10% = 100K
             *
             * or based on loglog(sAge); n = loglog(2^32) -> 32 -> 5 ; range = 1(or 2)  to  (n-1)
             * We can ignore first 'r' and last 'k' bins; divvy up rest in (n - (r+k))/Age.size)
             * from 2^2^0 - 2^2^1, 2^2^1 - 2^2^2, 2^2^2 - 2^2^3, 2^2^3 - 2^2^4, 2^2^4 - 2^2^5
             * bin 1: 2-4; bin 2: 4-32; bin 3: 32-256, bin 4: 256-65536, bin 5: 65536-4294967296;
             */

            lengthBSize = (log(2,log(2,sAge)))/Length.size;
            System.out.println("Length Bucket Size: " + lengthBSize + "  Length.size: " + Length.size);

            for(int i=0; i<Length.size-1; i++) {
                testLengthBuckets[i] =  pow(2,(pow(2, lengthBSize * (i+1))));
                System.out.println("Length Bucket " + i + ": " + testLengthBuckets[i]);
            }
            testLengthBuckets[Length.size-1]=sLength; // last bucket gets leftover; potentially larger than other windows
            System.out.println("Length Bucket " + (Length.size-1) + ": " + testLengthBuckets[Length.size-1]);

            // range query t1 = age-length/2; t2 = age+length/2
        }
        else {
            throw new StreamException("querying empty stream " + testSID);
        }

    }


    /* For now uniformly sample between the markers for the corresponding age and length classes
        Given an alclass, convert to (age, length) classes, assuming age-major ordering
     */

    public static Pair<Double> returnAgeLengthBiased(int alClass, StreamID streamID) {

        int aclass=0, lclass=0;
        double age=0d, length=0d;
        System.out.println("Computing Random Age Length for class: " + alClass + " ageClass: "
                + alClass/Length.size + " lengthClass: " + alClass%Length.size);

        // use testAgeBuckets[alClass/Length.size] and testLengthBuckets[alClass%Length.size]

        //System.out.println("Age: " + age + "; Length: " + length);
        return new Pair<Double>(age, length);
    }


    public static void main(String[] args) throws InterruptedException, StreamException, IOException, QueryException, LandmarkEventException, RocksDBException {

        AgeLengthClasses alc;
        StreamID testSID = new StreamID(100);
        Vector<Double> pValues = new Vector();
        int age_classes=1, len_classes=1, i=0, token=0;
        boolean eof = false;

        try {

            if (args.length == 2) {
                if (args[0].equalsIgnoreCase("-f")) {
                    Reader r = new BufferedReader(new FileReader(args[1]));
                    StreamTokenizer st = new StreamTokenizer(r);
                    st.nextToken();
                    age_classes = (int)st.nval;
                    st.nextToken();
                    len_classes = (int)st.nval;
                    do {
                        token = st.nextToken();
                        i++;

                        switch (token) {
                            case StreamTokenizer.TT_WORD:
                                System.out.println("Word: " + st.sval);
                                break;
                            case StreamTokenizer.TT_NUMBER:
                                //System.out.println("Number: " + st.nval);
                                pValues.add(st.nval);
                                break;
                            default:
                                //System.out.println((char) token + " encountered.");
                        }

                        if (i == age_classes*len_classes || token == '!') {
                            //System.out.println("eof: " + i);
                            eof = true;
                        }

                    } while (!eof);
                    if (token == '!' && pValues.size() < age_classes * len_classes) { // entry to indicate same p values for all
                        for (int j = pValues.size(); j < age_classes * len_classes; j++)
                            pValues.add(st.nval);
                    }

                    System.out.println("pValues vector with entries: " + pValues.size() + " : " + pValues.toString());
                }
            }

        }
        catch(Exception ex)
        {
        ex.printStackTrace();
        }

        alc = new AgeLengthClasses(pValues);
        alc.createTestBuckets(testSID);

        for (i=0; i<10; i++) {
            alc.returnAgeLengthBiased(alc.selectRandomAgeLengthClass(), testSID);
        }
    }

}
