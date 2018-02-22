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
