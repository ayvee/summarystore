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
package com.clearspring.analytics.stream.membership;

import com.google.protobuf.ByteString;
import com.samsung.sra.protocol.SummaryStore.ProtoOperator;

import java.util.BitSet;

public class BFProtofier {
    public static ProtoOperator.Builder protofy(BloomFilter bf) {
        // little-endian repr of bf's bitset. toByteArray() drops most-significant zeroes
        ByteString bstr = ByteString.copyFrom(bf.filter().toByteArray());
        return ProtoOperator
                .newBuilder()
                .setBytearray(bstr);
    }

    public static BloomFilter deprotofy(ProtoOperator op, int nrHashes, int filterSize) {
        BitSet bset = BitSet.valueOf(op.getBytearray().toByteArray());
        if (bset.size() < filterSize) { // protofy could have dropped most-significant zeros; restore them if it has
            BitSet ext = new BitSet(filterSize);
            ext.or(bset);
            bset = ext;
        }
        BloomFilter bf = new BloomFilter(nrHashes, bset);
        assert bf.buckets() == filterSize;
        return bf;
    }

    public static BloomFilter createEmpty(int nrHashes, int filterSize) {
        return new BloomFilter(nrHashes, new BitSet(filterSize));
    }
}
