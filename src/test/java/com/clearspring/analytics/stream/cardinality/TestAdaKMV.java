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

import com.clearspring.analytics.TestUtils;
import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAdaKMV {


    @Test
    public void testComputeCount() {
        AdaKMV adaKMV = new AdaKMV(16);
        adaKMV.offer(0);
        adaKMV.offer(1);
        adaKMV.offer(2);
        adaKMV.offer(3);
        adaKMV.offer(16);
        adaKMV.offer(17);
        adaKMV.offer(18);
        adaKMV.offer(19);
        adaKMV.offer(19);
        assertEquals(8, adaKMV.cardinality());
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
    @Test
    public void testHighCardinality() {
        long start = System.currentTimeMillis();
        AdaKMV adaKMV = new AdaKMV(1024);
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


    @Test
    public void testMergeSameThreshold() throws CardinalityMergeException {
        int numToMerge = 5;
        int threshold = 1024;
        int cardinality = 1000000;

        AdaKMV[] adaKMVs = new AdaKMV[numToMerge];
        AdaKMV baseline = new AdaKMV(threshold);
        for (int i = 0; i < numToMerge; i++) {
            adaKMVs[i] = new AdaKMV(threshold);
            for (int j = 0; j < cardinality; j++) {
                double val = Math.random();
                adaKMVs[i].offer(val);
                baseline.offer(val);
            }
        }


        long expectedCardinality = numToMerge * cardinality;
        AdaKMV adaKMV = adaKMVs[0];
        adaKMVs = Arrays.asList(adaKMVs).subList(1, adaKMVs.length).toArray(new AdaKMV[0]);
        long mergedEstimate = adaKMV.merge(adaKMVs).cardinality();
        long baselineEstimate = baseline.cardinality();
        double se = expectedCardinality * 0.1; // TODO: find a more accurate way of computing se

        System.out.println("Baseline estimate: " + baselineEstimate);
        System.out.println("Expect estimate: " + mergedEstimate + " is between " + (expectedCardinality - (3 * se)) + " and " + (expectedCardinality + (3 * se)));

        assertTrue(mergedEstimate >= expectedCardinality - (3 * se));
        assertTrue(mergedEstimate <= expectedCardinality + (3 * se));
        assertEquals(mergedEstimate, baselineEstimate);
    }

    @Test
    public void testMergeDifferentThreshold() throws CardinalityMergeException {
        int numToMerge = 5;
        int threshold = 128;
        int cardinality = 1000000;

        AdaKMV[] adaKMVs = new AdaKMV[numToMerge];
        AdaKMV baseline = new AdaKMV(threshold*6);
        for (int i = 0; i < numToMerge; i++) {
            adaKMVs[i] = new AdaKMV(threshold*(6-i));
            for (int j = 0; j < cardinality; j++) {
                double val = Math.random();
                adaKMVs[i].offer(val);
                baseline.offer(val);
            }
        }


        long expectedCardinality = numToMerge * cardinality;
        AdaKMV adaKMV = adaKMVs[0];
        adaKMVs = Arrays.asList(adaKMVs).subList(1, adaKMVs.length).toArray(new AdaKMV[0]);
        long mergedEstimate = adaKMV.merge(adaKMVs).cardinality();
        long baselineEstimate = baseline.cardinality();
        double se = expectedCardinality * 0.1; // TODO: find a more accurate way of computing se

        System.out.println("Baseline estimate: " + baselineEstimate);
        System.out.println("Expect estimate: " + mergedEstimate + " is between " + (expectedCardinality - (3 * se)) + " and " + (expectedCardinality + (3 * se)));
        System.out.println("Estimate error: " + Math.abs(expectedCardinality - mergedEstimate) /
                (double) expectedCardinality);


        assertTrue(mergedEstimate >= expectedCardinality - (3 * se));
        assertTrue(mergedEstimate <= expectedCardinality + (3 * se));
        assertEquals(mergedEstimate, baselineEstimate);
    }


}
