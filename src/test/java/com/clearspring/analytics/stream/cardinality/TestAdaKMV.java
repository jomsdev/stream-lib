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

    // TODO: Rewrite or reuse all the tests    @Test
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

    @Test
    public void testHighCardinality() {
        long start = System.currentTimeMillis();
        AdaKMV adaKMV = new AdaKMV(10);
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
    public void testMerge() throws CardinalityMergeException {
        int numToMerge = 5;
        int bits = 16;
        int cardinality = 1000000;

        AdaKMV[] adaKMVs = new AdaKMV[numToMerge];
        AdaKMV baseline = new AdaKMV(bits);
        for (int i = 0; i < numToMerge; i++) {
            adaKMVs[i] = new AdaKMV(bits);
            for (int j = 0; j < cardinality; j++) {
                double val = Math.random();
                adaKMVs[i].offer(val);
                baseline.offer(val);
            }
        }


        long expectedCardinality = numToMerge * cardinality;
        AdaKMV hll = adaKMVs[0];
        adaKMVs = Arrays.asList(adaKMVs).subList(1, adaKMVs.length).toArray(new AdaKMV[0]);
        long mergedEstimate = hll.merge(adaKMVs).cardinality();
        long baselineEstimate = baseline.cardinality();
        double se = expectedCardinality * (1.04 / Math.sqrt(Math.pow(2, bits)));

        System.out.println("Baseline estimate: " + baselineEstimate);
        System.out.println("Expect estimate: " + mergedEstimate + " is between " + (expectedCardinality - (3 * se)) + " and " + (expectedCardinality + (3 * se)));

        assertTrue(mergedEstimate >= expectedCardinality - (3 * se));
        assertTrue(mergedEstimate <= expectedCardinality + (3 * se));
        assertEquals(mergedEstimate, baselineEstimate);
    }

    /**
     * should not fail with AdaKMVMergeException: "Cannot merge estimators of different sizes"
     */
    @Test
    public void testMergeWithRegisterSet() throws CardinalityMergeException {
        AdaKMV first = new AdaKMV(16, new RegisterSet(1 << 20));
        AdaKMV second = new AdaKMV(16, new RegisterSet(1 << 20));
        first.offer(0);
        second.offer(1);
        first.merge(second);
    }

    @Test
    @Ignore
    public void testPrecise() throws CardinalityMergeException {
        int cardinality = 1000000000;
        int b = 12;
        AdaKMV baseline = new AdaKMV(b);
        AdaKMV guava128 = new AdaKMV(b);
        HashFunction hf128 = Hashing.murmur3_128();
        for (int j = 0; j < cardinality; j++) {
            Double val = Math.random();
            String valString = val.toString();
            baseline.offer(valString);
            guava128.offerHashed(hf128.hashString(valString, Charsets.UTF_8).asLong());
            if (j > 0 && j % 1000000 == 0) {
                System.out.println("current count: " + j);
            }
        }


        long baselineEstimate = baseline.cardinality();
        long g128Estimate = guava128.cardinality();
        double se = cardinality * (1.04 / Math.sqrt(Math.pow(2, b)));
        double baselineError = (baselineEstimate - cardinality) / (double) cardinality;
        double g128Error = (g128Estimate - cardinality) / (double) cardinality;
        System.out.format("b: %f g128 %f", baselineError, g128Error);
        assertTrue("baseline estimate bigger than expected", baselineEstimate >= cardinality - (2 * se));
        assertTrue("baseline estimate smaller than expected", baselineEstimate <= cardinality + (2 * se));
        assertTrue("g128 estimate bigger than expected", g128Estimate >= cardinality - (2 * se));
        assertTrue("g128 estimate smaller than expected", g128Estimate <= cardinality + (2 * se));
    }
}
