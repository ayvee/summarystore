/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package com.samsung.sra.DataStore.Aggregates;

import org.apache.commons.io.input.CountingInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

import java.util.BitSet;

public class BitSetSerializer {

    private static Logger logger = LoggerFactory.getLogger(BitSetSerializer.class);

    public static void serialize(BitSet bs, DataOutputStream dos) throws IOException {

        //NA: only for testing
        //ByteCounterOutputStream bcos = new ByteCounterOutputStream(dos);

        ObjectOutputStream oos = new ObjectOutputStream(dos); //bcos);
        //int start = bcos.bytesWrittenSoFar();
        oos.writeObject(bs);
        //int objectsize = bcos.bytesWrittenSoFar() - start;
        //System.out.println("Obj occupies " + objectsize + " bytes when serialized");

        oos.flush();
    }

    public static BitSet deserialize(DataInputStream dis) throws IOException {
        ObjectInputStream ois = new ObjectInputStream(dis);
        //NA: only for testing
        //CountingInputStream cIn = new CountingInputStream(dis);
        //logger.debug("====BitSet Read Bytes before: " + cIn.getByteCount());

        try {
            //NA: only for testing
            //BitSet tempBS = new BitSet ();
            //tempBS = (BitSet) ois.readObject();
            //logger.debug("====Bitset Read Bytes after: " + cIn.getByteCount());
            //return tempBS;

            return (BitSet) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}

class ByteCounterOutputStream extends DataOutputStream {
    public ByteCounterOutputStream(OutputStream out) {
        super(out);
    }
    public int bytesWrittenSoFar() {
        return written;
    }
}