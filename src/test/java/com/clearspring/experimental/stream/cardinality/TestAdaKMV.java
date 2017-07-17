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

package com.clearspring.experimental.stream.cardinality;

import com.clearspring.analytics.stream.cardinality.TestICardinality;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAdaKMV {

    @Test
    public void testComputeCount() {
        AdaKMV adaKMV = new AdaKMV(16, false);
        adaKMV.offer(0);
        adaKMV.offer(1);
        adaKMV.offer(2);
        adaKMV.offer(3);
        adaKMV.offer(16);
        adaKMV.offer(17);
        adaKMV.offer(18);
        adaKMV.offer(19);
        assertEquals(8, adaKMV.cardinality());
    }

    @Test
    public void testComputeCountWithRepetitions() {
        AdaKMV adaKMV = new AdaKMV(16, false);
        adaKMV.offer(0);
        adaKMV.offer(1);
        adaKMV.offer(2);
        adaKMV.offer(3);
        adaKMV.offer(16);
        adaKMV.offer(17);
        adaKMV.offer(18);
        adaKMV.offer(19);
        adaKMV.offer(0);
        adaKMV.offer(16);
        adaKMV.offer(19);
        adaKMV.offer(0);
        assertEquals(8, adaKMV.cardinality());
    }

    @Test
    public void testHighCardinality() {
        long start = System.currentTimeMillis();
        AdaKMV adaKMV = new AdaKMV(10, false);
        int size = 10000000;
        for (int i = 0; i < size; i++) {
            adaKMV.offer(TestICardinality.streamElement(i));
        }
        System.out.println("time: " + (System.currentTimeMillis() - start));
        long estimate = adaKMV.cardinality();
        double err = Math.abs(estimate - size) / (double) size;
        System.out.println(err);
        assertTrue(err < .1);
    }
/*
    @Test
    public void testHighCardinality_withDefinedRSD() {
        long start = System.currentTimeMillis();
        HyperLogLog hyperLogLog = new HyperLogLog(0.01);
        int size = 100000000;
        for (int i = 0; i < size; i++) {
            hyperLogLog.offer(TestICardinality.streamElement(i));
        }
        System.out.println("time: " + (System.currentTimeMillis() - start));
        long estimate = hyperLogLog.cardinality();
        double err = Math.abs(estimate - size) / (double) size;
        System.out.println(err);
        assertTrue(err < .1);
    }
    */
}
