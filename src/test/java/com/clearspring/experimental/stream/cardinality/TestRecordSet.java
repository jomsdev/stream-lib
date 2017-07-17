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

import java.util.Random;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestRecordSet {

    @Test
    public void testRecords() throws Exception {
        int size = (int) Math.pow(2, 2);
        RecordSet rs = new RecordSet(size, false);

        for (int i = 12; i >= 0; --i) {
            rs.insertIfRecord(new Long(i));
        }

        assertEquals(size, rs.records.size());
        assertEquals(0, rs.oldRecords.size());

        assertEquals(new Long(0), rs.records.first());
        assertEquals(new Long(3), rs.records.last());
    }

    @Test
    public void testRecordsGrowth() throws Exception {
        int size = (int) Math.pow(2, 2);
        RecordSet rs = new RecordSet(size, true);

        // Fill with records
        rs.insertIfRecord(new Long(10));
        rs.insertIfRecord(new Long(9));
        rs.insertIfRecord(new Long(7));
        rs.insertIfRecord(new Long(6));


        // Insert two new records
        rs.insertIfRecord(new Long(0));
        rs.insertIfRecord(new Long(1));


        // Check sizes (it has grown by 2)
        assertEquals(size, rs.records.size());
        assertEquals(2, rs.oldRecords.size());

        // Insert two new recrods that are not records
        rs.insertIfRecord(new Long(11));
        rs.insertIfRecord(new Long(12));

        // Check that it did not grown
        assertEquals(2, rs.oldRecords.size());

        // Add a new record, but not in the first set (it does not grow)
        rs.insertIfRecord(new Long(8));
        assertEquals(2, rs.oldRecords.size());
        assertEquals(new Long(8), rs.oldRecords.first());
    }

    @Test
    public void testRecordsWitouthReplacement() throws Exception {
        int size = (int) Math.pow(2, 2);
        RecordSet rs = new RecordSet(size, true);

        // Fill with records
        rs.insertIfRecord(new Long(10));
        rs.insertIfRecord(new Long(9));
        rs.insertIfRecord(new Long(7));
        rs.insertIfRecord(new Long(6));


        // Insert two new records
        rs.insertIfRecord(new Long(0));
        rs.insertIfRecord(new Long(1));

        rs.insertIfRecord(new Long(8));
        assertEquals(2, rs.oldRecords.size());
        assertEquals(new Long(8), rs.oldRecords.first());
    }
}
