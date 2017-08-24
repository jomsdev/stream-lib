/*
 * Copyright (C) 2011 Clearspring Technologies, Inc. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clearspring.analytics.stream.cardinality;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRecordSet {


    @Test
    public void testNormalGrowth() {
        RecordSet rs = new RecordSet(4, 0, 10,
                new TreeSet<Long>(), new TreeSet<Long>());
        assertEquals(rs.currentSize(), 0);
        rs.offer(11);
        rs.offer(11);
        rs.offer(11);
        rs.offer(9);
        rs.offer(8);
        rs.offer(7);
        rs.offer(6);
        rs.offer(12);
        rs.offer(13);
        assertEquals(rs.currentSize(), 5);

    }

    @Test
    public void testPseudoCorrectReplacement() {
        RecordSet rs = new RecordSet(4, 0, 10,
                new TreeSet<Long>(), new TreeSet<Long>());
        rs.offer(10);
        rs.offer(4);
        rs.offer(3);
        rs.offer(2);
        rs.offer(1);
        rs.offer(7);
        rs.offer(6);
        rs.offer(5);
        assertEquals(rs.currentSize(), 5);

    }

    @Test
    public void testMaxSize() {
        RecordSet rs = new RecordSet(5, 0, 10,
                new TreeSet<Long>(), new TreeSet<Long>());
        RecordSet rs2 = new RecordSet(10, 0, 10,
                new TreeSet<Long>(), new TreeSet<Long>());
        for (int i = 11; i >= 0; i--){
            rs.offer(i);
            rs2.offer(i);
        }
        assertEquals(rs.currentSize(), 10);
        assertEquals(rs.currentSize(), 10);
    }

    @Ignore
    @Test
    public void testContains() {
        RecordSet rs = new RecordSet(9, 0, 10,
                new TreeSet<Long>(), new TreeSet<Long>());
        for (int i = 0; i < 10; i++){
            rs.offer(i);
            assertTrue(rs.contains(i));
            assertTrue(!rs.contains(i+20));
        }

    }

    
    /*
    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        AdaKMV hll = new AdaKMV(8);
        hll.offer("a");
        hll.offer("b");
        hll.offer("c");
        hll.offer("d");
        hll.offer("e");

        AdaKMV hll2 = (AdaKMV) TestUtils.deserialize(TestUtils.serialize(hll));
        assertEquals(hll.cardinality(), hll2.cardinality());
    }

    @Test
    public void testSerializationUsingBuilder() throws IOException {
        AdaKMV hll = new AdaKMV(8);
        hll.offer("a");
        hll.offer("b");
        hll.offer("c");
        hll.offer("d");
        hll.offer("e");

        AdaKMV hll2 = AdaKMV.Builder.build(hll.getBytes());
        assertEquals(hll.cardinality(), hll2.cardinality());
    }
    */


}
