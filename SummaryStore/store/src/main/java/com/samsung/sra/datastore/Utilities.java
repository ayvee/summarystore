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
package com.samsung.sra.datastore;

import org.apache.commons.math3.distribution.NormalDistribution;

import java.io.*;
import java.util.concurrent.BlockingQueue;

public class Utilities {
    private Utilities() {}

    /** stuff val into array[startPos], array[startPos+1], ..., array[startPos+7] */
    public static void longToByteArray(long val, byte[] array, int startPos) {
        array[startPos    ] = (byte) ((val >> 56) & 0xFFL);
        array[startPos + 1] = (byte) ((val >> 48) & 0xFFL);
        array[startPos + 2] = (byte) ((val >> 40) & 0xFFL);
        array[startPos + 3] = (byte) ((val >> 32) & 0xFFL);
        array[startPos + 4] = (byte) ((val >> 24) & 0xFFL);
        array[startPos + 5] = (byte) ((val >> 16) & 0xFFL);
        array[startPos + 6] = (byte) ((val >> 8)  & 0xFFL);
        array[startPos + 7] = (byte)  (val        & 0xFFL);
    }

    /** return the long represented by array[startPos], array[startPos+1], ..., array[startPos+7] */
    public static long byteArrayToLong(byte[] array, int startPos) {
        return
                (((long) array[startPos    ] & 0xFFL) << 56) |
                (((long) array[startPos + 1] & 0xFFL) << 48) |
                (((long) array[startPos + 2] & 0xFFL) << 40) |
                (((long) array[startPos + 3] & 0xFFL) << 32) |
                (((long) array[startPos + 4] & 0xFFL) << 24) |
                (((long) array[startPos + 5] & 0xFFL) << 16) |
                (((long) array[startPos + 6] & 0xFFL) << 8) |
                ((long)  array[startPos + 7] & 0xFFL);
    }

    private static final NormalDistribution normalDist = new NormalDistribution(0, 1);

    public static double getNormalQuantile(double P) {
        return normalDist.inverseCumulativeProbability(P);
    }

    /** Blocking get ignoring InterruptedExceptions */
    public static <T> T take(BlockingQueue<T> queue) {
        while (true) {
            try {
                return queue.take();
            } catch (InterruptedException ignored) {
            }
        }
    }

    /** Blocking put ignoring InterruptedExceptions */
    public static <T> void put(BlockingQueue<T> queue, T value) {
        while (true) {
            try {
                queue.put(value);
                return;
            } catch (InterruptedException ignored) {
            }
        }
    }

    public static <T> byte[] serialize(T obj) throws IOException {
        try(ByteArrayOutputStream b = new ByteArrayOutputStream()){
            try(ObjectOutputStream o = new ObjectOutputStream(b)){
                o.writeObject(obj);
            }

            return b.toByteArray();
        }
    }

    public static <T> T deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        try(ByteArrayInputStream b = new ByteArrayInputStream(bytes)){
            try(ObjectInputStream o = new ObjectInputStream(b)){
                return (T) o.readObject();
            }
        }
    }

    public static <T> void serializeToFile(String filename, T obj) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filename))) {
            oos.writeObject(obj);
        }
    }

    public static <T> T deserializeFromFile(String filename) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream((new FileInputStream(filename)))) {
            return (T) ois.readObject();
        }
    }
}
