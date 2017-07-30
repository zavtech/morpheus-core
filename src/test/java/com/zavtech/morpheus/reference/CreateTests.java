/**
 * Copyright (C) 2014-2017 Xavier Witdouck
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
package com.zavtech.morpheus.reference;

import java.time.LocalDate;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameAsserts;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.range.Range;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.testng.annotations.Test;

/**
 * A unit test to exercise the various mechanisms for constructing DataFrames.
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class CreateTests {

    final Index<String> rowKeys = Index.of("R1", "R2", "R3", "R4", "R5", "R6", "R7");
    final DataFrame<String,String> template = DataFrame.of(rowKeys, String.class, columns -> {
        columns.add("C1", Double.class);
        columns.add("C2", Integer.class);
        columns.add("C3", Boolean.class);
        columns.add("C4", Double.class);
        columns.add("C5", Long.class);
        columns.add("C6", Object.class);
    });


    @Test()
    public void testConstruction1() {
        final DataFrame<String,String> frame1 = template.copy();
        final DataFrame<String,String> frame2 = template.copy();
        final DataFrame<String,String> frame3 = template.copy();

        frame1.colAt("C1").applyDoubles(v -> Math.random());
        frame1.colAt("C2").applyInts(v -> (int) (Math.random() * 100));
        frame1.colAt("C3").applyBooleans(v -> Math.random() > 0.5);
        frame1.colAt("C4").applyDoubles(v -> Math.random());
        frame1.colAt("C5").applyLongs(v -> (long) (Math.random() * 100000));
        frame1.colAt("C6").applyValues(v -> String.valueOf(Math.random()));

        frame2.colAt("C1").applyDoubles(v -> frame1.data().getDouble(v.rowKey(), "C1"));
        frame2.colAt("C2").applyInts(v -> frame1.data().getInt(v.rowKey(), "C2"));
        frame2.colAt("C3").applyBooleans(v -> frame1.data().getBoolean(v.rowKey(), "C3"));
        frame2.colAt("C4").applyDoubles(v -> frame1.data().getDouble(v.rowKey(), "C4"));
        frame2.colAt("C5").applyLongs(v -> frame1.data().getLong(v.rowKey(), "C5"));
        frame2.colAt("C6").applyValues(v -> frame1.data().getValue(v.rowKey(), "C6"));

        frame3.rowAt("R1").applyValues(v -> frame1.data().getValue("R1", v.colKey()));
        frame3.rowAt("R2").applyValues(v -> frame1.data().getValue("R2", v.colKey()));
        frame3.rowAt("R3").applyValues(v -> frame1.data().getValue("R3", v.colKey()));
        frame3.rowAt("R4").applyValues(v -> frame1.data().getValue("R4", v.colKey()));
        frame3.rowAt("R5").applyValues(v -> frame1.data().getValue("R5", v.colKey()));
        frame3.rowAt("R6").applyValues(v -> frame1.data().getValue("R6", v.colKey()));
        frame3.rowAt("R7").applyValues(v -> frame1.data().getValue("R7", v.colKey()));

        DataFrameAsserts.assertEqualsByIndex(frame1, frame2);
        DataFrameAsserts.assertEqualsByIndex(frame1, frame3);
        DataFrameAsserts.assertEqualsByIndex(frame2, frame3);
    }


    @Test()
    public void testMapCreateTest() {
        final long t1 = System.nanoTime();
        final TObjectIntMap<String> map1 = new TObjectIntHashMap<>(5000000, 0.8f, -1);
        final long t2 = System.nanoTime();
        final TLongIntMap map2 = new TLongIntHashMap();
        final long t3 = System.nanoTime();
        System.out.println("Map1:" + ((t2-t1)/1000000d) + " Map2:" + ((t3-t2)/100000d));
    }


    @Test()
    public void testDateCreation() {
        final long t1 = System.nanoTime();
        final LocalDate start = LocalDate.of(1900, 1, 1);
        final Index<LocalDate> dates = Range.of(0, 5000000).map(start::plusDays).toIndex(LocalDate.class);
        final long t2 = System.nanoTime();
        final Index<String> colKeys = Index.of("C1", "C2", "C3", "C4", "C5");
        final long t3 = System.nanoTime();
        final DataFrame<LocalDate,String> frame = DataFrame.ofDoubles(dates, colKeys);
        final long t4 = System.nanoTime();
        System.out.println("Dates created in " + ((t2-t1)/1000000d) + " millis, header in " + ((t3-t2)/1000000d) + " millis, frame in " + ((t4-t3)/1000000d) + " millis");
    }

}
