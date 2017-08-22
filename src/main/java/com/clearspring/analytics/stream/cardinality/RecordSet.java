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


    private int size;
    private TreeSet<Long>  records;

    public RecordSet(int size) {
        this.size = size;
        this.records = new TreeSet<Long>();
    }

    public RecordSet(TreeSet<Long> initialValues) {
        this.size = initialValues.size();
        this.records = initialValues;
    }

    public int size(){
        return size;
    }

    // TODO: check name
    public boolean updateIfNecessary(long value) {
        boolean cardinalityAffected = false;

        if (!records.contains(value)){
            if(records.size() < size || value < records.last()){
                records.add(value);
                cardinalityAffected = true;
            }
            if(records.size() > size){
                records.remove(records.last());
            }
        }

        return cardinalityAffected;
    }

    public void merge(RecordSet that) {
        // TODO: check this is correct when record size is fixed
        for(Long record : that.records()){
            this.updateIfNecessary(record);
        }
    }

    public TreeSet<Long> records(){
        return records;
    }
}
