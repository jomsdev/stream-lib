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


import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.ICardinality;

import java.io.IOException;


public class AdaKMV implements ICardinality {

    private final double relativeZero = (double) Long.MIN_VALUE;
    private final double halfOfDistance = (double) Long.MAX_VALUE;

    private final int size;
    private final RecordSet recordSet;

    public AdaKMV(int size, boolean grow) {
        this.size = size;
        recordSet = new RecordSet(size, grow);
    }

    public int size() {
        return this.size;
    }

    @Override
    //TODO: Fix this function
    public long cardinality() {
        if (this.recordSet.records.size() < this.size) {
            return this.recordSet.records.size();
        }

        long farthestValue = this.recordSet.records.last();
        if (farthestValue < 0) {
            double distance = farthestValue + Long.MAX_VALUE;
            return (long) (2.0 / (distance / (double) Long.MAX_VALUE));
        } else {
            double offset_distance = farthestValue - Long.MAX_VALUE;
            return (long) (2.0 / (0.5 + (offset_distance / Long.MAX_VALUE)));
        }
    }

    @Override
    public boolean offerHashed(long hashedValue) {
        return this.recordSet.insertIfRecord(hashedValue);
    }

    @Override
    public boolean offerHashed(int hashedValue) {
        return offerHashed((long) hashedValue);
    }

    @Override
    public boolean offer(Object o) {
        long x = MurmurHash.hash64(o);
        return offerHashed(x);
    }

    @Override
    public int sizeof() {
        return this.recordSet.records.size() + this.recordSet.oldRecords.size();
    }

    @Override
    public byte[] getBytes() throws IOException {
        throw new IOException("getByte not implemented for AdaKMV yet");
    }

    @Override
    public ICardinality merge(ICardinality... estimators) throws CardinalityMergeException {
        throw new AdaKMVMergeException("Cannot merge AdaKMV estimators yet, feature not implemented");
    }

    @SuppressWarnings("serial")
    protected static class AdaKMVMergeException extends CardinalityMergeException {
        public AdaKMVMergeException(String message) {
            super(message);
        }
    }
}
