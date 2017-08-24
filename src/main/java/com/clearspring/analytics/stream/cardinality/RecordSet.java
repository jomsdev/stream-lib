/*
 * Copyright (C) 2012 Clearspring Technologies, Inc. // TODO: check copyright, license or whatever
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

import java.util.TreeSet;

public class RecordSet {


    private int replacementThreshold;
    private int maxSize;
    private int recordCounter = 0;
    private TreeSet<Long>  records;
    private TreeSet<Long>  pseudoRecords;

//    /* Constructor used for non-adaptative version of KMV
//    * */
//    public RecordSet(int replacementThreshold) {
//        this.replacementThreshold = replacementThreshold;
//        this.maxSize = replacementThreshold;
//        this.records = new TreeSet<Long>();
//    }

    public RecordSet(int replacementThreshold, int recordCounter, int maxSize, TreeSet<Long>
            initialValues, TreeSet<Long> initialPseudos) {
        this.recordCounter = recordCounter;
        this.replacementThreshold = replacementThreshold;
        this.maxSize = maxSize;
        this.records = initialValues;
        this.pseudoRecords = initialPseudos;
    }

    public RecordSet(RecordSet other) {
        this.recordCounter = other.recordCounter;
        this.replacementThreshold = other.replacementThreshold;
        this.maxSize = other.maxSize;
        this.records = new TreeSet<>(other.records);
        this.pseudoRecords = new TreeSet<>(other.pseudoRecords);
    }

    public int currentSize(){
        return records.size() + pseudoRecords.size();
    }

    public boolean offer(long value) {
        boolean cardinalityAffected = false;

        if (!this.contains(value)){
            if(records.size() < replacementThreshold || value < records.last()){
                records.add(value);
                ++recordCounter;
                cardinalityAffected = true;
            } else if (!pseudoRecords.isEmpty() && value < pseudoRecords.last()){
                pseudoRecords.add(value);
                pseudoRecords.remove(pseudoRecords.last());
                cardinalityAffected = true;
            }
            if (records.size() > replacementThreshold){
                pseudoRecords.add(records.last());
                records.remove(records.last());
            }
            if (currentSize() > maxSize){
                pseudoRecords.remove(pseudoRecords.last());
            }
        }

        return cardinalityAffected;
    }

    public void merge(RecordSet that) {
        for(Long record : that.records()){
            this.offer(record);
        }
    }

    public boolean contains(long value){
        return this.records.contains(value) || this.pseudoRecords.contains(value);
    }

    public Long maxValue() throws UnsupportedOperationException {
        long maxValue;

        if (!pseudoRecords.isEmpty()){
            maxValue = pseudoRecords.last();
        } else if (!records.isEmpty()){
            maxValue = records.last();
        } else {
            throw new UnsupportedOperationException("No records in record set.");
        }

        return maxValue;
    }

    public int replacementThreshold(){
        return replacementThreshold;
    }

    public TreeSet<Long> records(){
        return records;
    }
}
