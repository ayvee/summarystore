package com.clearspring.analytics.stream.frequency;

import com.samsung.sra.protocol.Summarybucket.ProtoCMS;

public class CMSProtofier {
    public static ProtoCMS.Builder protofy(CountMinSketch cms) {
        ProtoCMS.Builder builder = ProtoCMS.newBuilder();
        builder.setSize(cms.size);
        assert cms.hashA.length == cms.depth;
        for (int i = 0; i < cms.depth; ++i) {
            builder.addHashA(cms.hashA[i]);
        }
        assert cms.table.length == cms.depth;
        for (int i = 0; i < cms.depth; ++i) {
            ProtoCMS.Row.Builder rowBuilder = ProtoCMS.Row.newBuilder();
            for (int j = 0; j < cms.width; ++j) {
                rowBuilder.addCell(cms.table[i][j]);
            }
            builder.addRow(rowBuilder);
        }
        return builder;
    }

    public static CountMinSketch deprotofy(ProtoCMS protoCMS, int depth, int width) {
        long size = protoCMS.getSize();
        assert protoCMS.getHashACount() == depth;
        long[] hashA = new long[depth];
        for (int i = 0; i < depth; ++i) {
            hashA[i] = protoCMS.getHashA(i);
        }
        assert protoCMS.getRowCount() == depth;
        long[][] table = new long[depth][width];
        for (int i = 0; i < depth; ++i) {
            ProtoCMS.Row row = protoCMS.getRow(i);
            assert row.getCellCount() == width;
            for (int j = 0; j < width; ++j) {
                table[i][j] = row.getCell(j);
            }
        }
        // FIXME: bug in CMS code, uses int for size in constructor but field is declared as long
        return new CountMinSketch(depth, width, (int) size, hashA, table);
    }
}
