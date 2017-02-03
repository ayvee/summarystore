package com.clearspring.analytics.stream.membership;

import com.google.protobuf.ByteString;
import com.samsung.sra.protocol.SummaryStore.ProtoOperator;

import java.util.BitSet;

public class BFProtofier {
    public static ProtoOperator.Builder protofy(BloomFilter bf) {
        return ProtoOperator
                .newBuilder()
                .setBytearray(ByteString.copyFrom(bf.filter().toByteArray()));
    }

    public static BloomFilter deprotofy(ProtoOperator protoOperator, int nrHashes, int filterSize) {
        BloomFilter bf = new BloomFilter(nrHashes, BitSet.valueOf(protoOperator.getBytearray().toByteArray()));
        assert bf.buckets() == filterSize;
        return bf;
    }
}
