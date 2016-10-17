/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.samsung.sra.DataStore.Aggregates;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.BitSet;

public class BloomFilter extends Filter {

    static ICompactSerializer<BloomFilter> serializer_ = new BloomFilterSerializer();

    public static ICompactSerializer<BloomFilter> serializer() {
        return serializer_;
    }

    // FIXME: NA; change to private
    public BitSet filter_;

    private static Logger logger = LoggerFactory.getLogger(BloomFilter.class);

    public BloomFilter(BloomFilter a) {
        this.hashCount = a.hashCount;
        this.filterSize = a.filterSize;
        this.filter_ = a.filter_;
    }

    public BloomFilter(int numElements, int bucketsPerElement, int override) {
        this(BloomCalculations.computeBestK(bucketsPerElement), new BitSet(numElements * bucketsPerElement + 20));
    }

    public BloomFilter(int numElements, double maxFalsePosProbability) {
        BloomCalculations.BloomSpecification spec = BloomCalculations
                .computeBucketsAndK(maxFalsePosProbability);
        filter_ = new BitSet(numElements * spec.bucketsPerElement + 20);
        hashCount = spec.K;
    }

    public BloomFilter(int filterSize, int hashCount) {
        this.filterSize = filterSize;
        this.hashCount = hashCount;
        this.filter_ = new BitSet(this.filterSize);
    }

    /*
     * This version is only used by the deserializer.
     */
    BloomFilter(int hashes, BitSet filter) {
        hashCount = hashes;
        filter_ = filter;
    }

    BloomFilter(int hashes, BitSet filter, int fsize) {
        hashCount = hashes;
        filter_ = filter;
        filterSize = fsize;
    }

    public void clear() {
        filter_.clear();
    }

    public int buckets() {
        return filter_.size();
    }

    BitSet filter() {
        return filter_;
    }

    public boolean isPresent(String key) {
        for (int bucketIndex : getHashBuckets(key)) {
            if (!filter_.get(bucketIndex)) {
                return false;
            }
        }
        return true;
    }

    public boolean isPresent(byte[] key) {
        for (int bucketIndex : getHashBuckets(key)) {
            if (!filter_.get(bucketIndex)) {
                return false;
            }
        }
        return true;
    }

    /*
     @param key -- value whose hash is used to fill
     the filter_.
     This is a general purpose API.
     */
    public void add(String key) {
        for (int bucketIndex : getHashBuckets(key)) {
            filter_.set(bucketIndex);
        }
    }

    public void add(byte[] key) {
        for (int bucketIndex : getHashBuckets(key)) {
            filter_.set(bucketIndex);
        }
    }

    public String toString() {
        return filter_.toString() + ":" + filter_.size();
    }

    ICompactSerializer tserializer() {
        return serializer_;
    }

    int emptyBuckets() {
        int n = 0;
        for (int i = 0; i < buckets(); i++) {
            if (!filter_.get(i)) {
                n++;
            }
        }
        return n;
    }

    public void addAll(BloomFilter other) {
        if (this.getHashCount() != other.getHashCount()) {
            throw new IllegalArgumentException("Cannot merge filters of different sizes");
        }

        this.filter().or(other.filter());
    }

    public Filter merge(Filter... filters) {
        BloomFilter merged = new BloomFilter(this.getHashCount(), (BitSet) this.filter().clone());

        if (filters == null) {
            return merged;
        }

        for (Filter filter : filters) {
            if (!(filter instanceof BloomFilter)) {
                throw new IllegalArgumentException("Cannot merge filters of different class");
            }
            BloomFilter bf = (BloomFilter) filter;
            merged.addAll(bf);
        }

        return merged;
    }

    public static BloomFilter unionOf(BloomFilter a, BloomFilter b) {


        String ret = "Union of A and B BFs: " + a.toString() + ":" + b.toString()
        + "\n filter:" + a.filter().toString() + ":" + b.filter().toString()
        + "\n filter:" + a.filter_ + ":" + b.filter_
        + "\n hashcount:" + a.getHashCount() + ":" + b.getHashCount()
        + "\n filtersize:" + a.getFilterSize() + ":" + b.getFilterSize()
        + "\n filtersize:" + a.filter_.size() + ":" + b.filter_.size()
                ;

        //logger.debug(ret);

        if(a.filterSize != b.filterSize || a.getHashCount() != b.getHashCount()) {
            throw new IllegalArgumentException("Cannot merge filters of different sizes");
        } else {
            //logger.debug("===== Valid union of BloomFilters");
            a.filter_.or(b.filter_);
            return a;
        }
    }

    /**
     * @return a BloomFilter that always returns a positive match, for testing
     */
    public static BloomFilter alwaysMatchingBloomFilter() {
        BitSet set = new BitSet(64);
        set.set(0, 64);
        return new BloomFilter(1, set);
    }

    public static byte[] serialize(BloomFilter filter) {
        DataOutputBuffer out = new DataOutputBuffer();
        try {
            BloomFilter.serializer().serialize(filter, out);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return out.getData();
    }

    public static BloomFilter deserialize(byte[] bytes) {
        BloomFilter filter = null;
        DataInputBuffer in = new DataInputBuffer();
        in.reset(bytes, bytes.length);
        try {
            filter = BloomFilter.serializer().deserialize(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return filter;
    }
}

class BloomFilterSerializer implements ICompactSerializer<BloomFilter> {

    private static Logger logger = LoggerFactory.getLogger(BloomFilterSerializer.class);

    public void serialize(BloomFilter bf, DataOutputStream dos)
            throws IOException {
        int start = dos.size();
        dos.writeInt(bf.getHashCount());
        BitSetSerializer.serialize(bf.filter(), dos);
        int end = dos.size();
    }

    public BloomFilter deserialize(DataInputStream dis) throws IOException {
        int hashes = 0;
        BitSet bs = null;

        //NA: only for testing
        //CountingInputStream cIn = new CountingInputStream(dis);
        //logger.debug("====Read Bytes before: " + cIn.getByteCount());

        try {
            hashes = dis.readInt();
            bs = (BitSet) BitSetSerializer.deserialize(dis);
        } catch (Exception e){
            e.printStackTrace();
        }

        //logger.debug("====Read Bytes after: " + cIn.getByteCount());
        return new BloomFilter(hashes, bs);
    }
}


