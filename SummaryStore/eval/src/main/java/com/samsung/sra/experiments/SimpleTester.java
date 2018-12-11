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
package com.samsung.sra.experiments;

import com.moandjiezana.toml.Toml;
import com.samsung.sra.datastore.*;
import com.samsung.sra.datastore.aggregates.BloomFilterOperator;
import com.samsung.sra.datastore.aggregates.SimpleCountOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;

/**
 * Created by n.agrawal1 on 7/21/16.
 */
public class SimpleTester {

    private static String directory = "/tmp/tdstore";
    private static long streamID = 0;
    public static Logger logger = LoggerFactory.getLogger(SimpleTester.class);


    public static void main(String[] args) throws Exception {

        Runtime.getRuntime().exec(new String[]{"sh", "-c",  "rm -rf " + directory}).waitFor();

        SummaryStore store = null;
        long T =100; //  entries
        //long storageSavingsFactor = 1;
        //long W = T / 2 / storageSavingsFactor; // # windows
        long W = T;
        long Q = 1000;

        LinkedHashMap<String, SummaryStore> stores = new LinkedHashMap<>();

        Windowing windowing
                = new GenericWindowing(new ExponentialWindowLengths(2));
                //= new RationalPowerWindowing(1, 2);


        try {
            store = new SummaryStore(directory);
            store.registerStream(streamID, windowing,
                    new SimpleCountOperator()
                    // new SimpleCountOperator(SimpleCountOperator.Estimator.PROPORTIONAL),
                    ,new BloomFilterOperator(7, 128)
                    );
            stores.put("expstore", store);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Testing a store with " + T + " elements");

        RandomStreamGenerator generator = new RandomStreamGenerator(new Toml().read(
                "interarrivals = {distribution = \"ExponentialDistribution\", lambda = 1.0}\n" +
                "values = {distribution = \"FixedDistribution\", value = 10}"
        ));
        long w0 = System.currentTimeMillis();
        //BiConsumer<Long, Long> printer = (ts, v) -> System.out.println(ts + "\t" + v);
        //BiConsumer<Long, Long> myvals = (ts, v) -> store.append(streamID, ts, v);

        generator.generate(0, T, op -> {
            assert op.type == StreamGenerator.Operation.Type.APPEND;
            try {
                //logger.debug("Inserting " + v + " at Time " + t);
                for (SummaryStore astore: stores.values()) {
                    astore.append(streamID, op.timestamp, op.value);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        long we = System.currentTimeMillis();
        //System.out.println("Write throughput = " + (T * 1000d / (we - w0)) + " appends/s" );

        store.printWindowState(streamID);
        //logger.debug("Size for store " + streamID + " : " + store.getStoreSizeInBytes());

        Object o[] = {new Long(50), new Long(0)};
        ResultError<Boolean,Double> re2 =
                (ResultError<Boolean,Double>) store.query(streamID, 0, T-1, 1, o[0]);
        logger.debug("Bloom.ispresent(" + o[0] + "): " + re2.result + " with error prob " + re2.error);


        Object o2[] = {new Long(82)};
        re2 = (ResultError<Boolean,Double>) store.query(streamID, 0, T-1, 1, o2[0]);
        logger.debug("Bloom.ispresent(" + o2[0] + "): " + re2.result + " with error prob " + re2.error);

        Object o3[] = {new Long(10)};
        re2 = (ResultError<Boolean,Double>) store.query(streamID, 0, T-1, 1, o3[0]);
        logger.debug("Bloom.ispresent(" + o3[0] + "): " + re2.result + " with error prob " + re2.error);

        /*
        ResultError<Double,Pair<Double, Double>> re =
                (ResultError<Double,Pair<Double, Double>>) store.query(streamID, 4928, 4975, 0, o[1]);
        logger.debug("Querying in stream " + streamID + " Time0: t0" + " Time1: t1; RE: " + re.result);


        long f0 = System.currentTimeMillis();
        store.query(streamID, 0, T-1, 0, o[1]);
        long fe = System.currentTimeMillis();
        System.out.println("Time to run query, spanning [0, T) = " + ((fe - f0) / 1000d) + " sec");
        */

        store.close();
    }
}
