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


import com.clearspring.analytics.stream.cardinality.RegisterSet;

import java.util.TreeSet;

public class RecordSet {

    public int size;
    private boolean grows;

    private int counter;
    public TreeSet<Long> records;
    public TreeSet<Long> oldRecords;

    public RecordSet(int size, boolean grows) {
        this(size, grows, 0, null, null);
    }

    public RecordSet(int size, boolean grows, int counter, TreeSet<Long> records, TreeSet<Long> oldRecords) {
        this.size = size;
        this.grows = grows;
        this.counter = counter;

        if (records == null) {
            this.records = new TreeSet<>();
        } else {
            this.records = records;
        }

        if (oldRecords == null) {
            this.oldRecords = new TreeSet<>();
        } else {
            this.oldRecords = oldRecords;
        }
    }

    public boolean insertIfRecord(long value) {
        boolean modified = false;

        if (!this.records.contains(value) && !this.oldRecords.contains(value)) {
            if (this.records.size() < this.size) {
                modified = true;
                this.records.add(value);
            } else if (this.records.last() > value) {
                modified = true;
                this.records.add(value);
                if (this.grows) {
                    this.oldRecords.add(this.records.last());
                }
                this.records.remove(this.records.last());
            } else if (this.grows && !this.oldRecords.isEmpty() && this.oldRecords.last() > value) {
                modified = true;
                this.oldRecords.add(value);
                this.oldRecords.remove(this.oldRecords.last());
            }
        }

        return modified;
    }
}
