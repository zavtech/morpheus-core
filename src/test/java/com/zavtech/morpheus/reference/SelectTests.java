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

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameAsserts;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameRow;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.range.Range;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test of the various <code>DataFrame</code> selection methods
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class SelectTests {

    private static int rowCount = 100;
    private static int colCount = 100;
    private static ZonedDateTime startTime = ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneId.of("GMT"));
    private static Index<ZonedDateTime> rows = Range.of(0, rowCount).map(startTime::plusDays).toIndex(ZonedDateTime.class);
    private static Index<String> columns = Range.of(0, colCount).map(i -> "C" + i).toIndex(String.class);


    @DataProvider(name = "frameTypes")
    public static Object[][] getFrameTypes() {
        return new Object[][] {
                { boolean.class },
                { int.class },
                { long.class },
                { double.class },
                { Object.class },
        };
    }


    @Test(dataProvider="frameTypes")
    public void testHeadSelection(Class type) {
        final DataFrame<ZonedDateTime,String> frame = TestDataFrames.random(type, rows, columns);
        for (int count : new int[] {20, 50, 200}) {
            final DataFrame<ZonedDateTime,String> head = frame.head(count);
            final Set<ZonedDateTime> rowKeys = IntStream.range(0,Math.min(frame.rowCount(), count)).mapToObj(rows::getKey).collect(Collectors.toSet());
            final DataFrame<ZonedDateTime,String> expected = frame.rows().select(row -> rowKeys.contains(row.key()));
            DataFrameAsserts.assertEqualsByIndex(expected, head);
        }
    }


    @Test(dataProvider="frameTypes")
    public void testTailSelection(Class type) {
        final DataFrame<ZonedDateTime,String> frame = TestDataFrames.random(type, rows, columns);
        for (int count : new int[] {20, 50, 200}) {
            final DataFrame<ZonedDateTime,String> tail = frame.tail(count);
            final Set<ZonedDateTime> rowKeys = IntStream.range(Math.max(0, frame.rowCount() - count), frame.rowCount()).mapToObj(rows::getKey).collect(Collectors.toSet());
            final DataFrame<ZonedDateTime,String> expected = frame.rows().select(row -> rowKeys.contains(row.key()));
            DataFrameAsserts.assertEqualsByIndex(expected, tail);
        }
    }


    @Test(dataProvider="frameTypes")
    public void testSelectRows(Class type) throws Exception {
        final ZonedDateTime first = startTime.plusDays(14);
        final ZonedDateTime last = startTime.plusDays(28);
        final Predicate<ZonedDateTime> predicate1 = d -> d.isAfter(first) && d.isBefore(last);
        final Predicate<DataFrameRow<ZonedDateTime,String>> predicate2 = row -> predicate1.test(row.key());
        final DataFrame<ZonedDateTime,String> frame = TestDataFrames.random(type, rows, columns);
        final DataFrame<ZonedDateTime,String> slice = frame.rows().select(predicate2);
        final Index<ZonedDateTime> rowKeys = Index.of(rows.keys().filter(predicate1).collect(Collectors.toList()));
        Assert.assertEquals(slice.rows().keyArray(), rowKeys.toArray(), "The row keys match");
        Assert.assertEquals(slice.cols().keyArray(), frame.cols().keyArray(), "The column keys match");
        final DataFrame<ZonedDateTime,String> expected = TestDataFrames.random(type, rowKeys, columns);
        expected.applyValues(v -> frame.data().getValue(v.rowKey(), v.colKey()));
        expected.forEachValue(v -> {
            final Object v1 = v.getValue();
            final Object v2 = frame.data().getValue(v.rowKey(), v.colKey());
            Assert.assertEquals(v1, v2, "The expected frame matches source for " + v);
        });

        slice.out().print();
        expected.out().print();
        DataFrameAsserts.assertEqualsByIndex(expected, slice);
    }



    @Test(dataProvider="frameTypes")
    public void testSelectColumns(Class type) {
        final Index<String> colKeys = Index.of("C2", "C5", "C19", "C27");
        final DataFrame<ZonedDateTime,String> frame = TestDataFrames.random(type, rows, columns);
        final DataFrame<ZonedDateTime,String> slice = frame.cols().select(column -> colKeys.contains(column.key()));
        final DataFrame<ZonedDateTime,String> expected = TestDataFrames.random(type, rows, colKeys);
        expected.applyValues(v -> frame.data().getValue(v.rowKey(), v.colKey()));
        DataFrameAsserts.assertEqualsByIndex(expected, slice);
    }


    @Test(dataProvider="frameTypes")
    public void testUnderlyingUpdatesVisibleToSelection(Class type) throws Exception {
        final ZonedDateTime first = startTime.plusDays(14);
        final ZonedDateTime last = startTime.plusDays(28);
        final DataFrame<ZonedDateTime,String> frame = TestDataFrames.random(type, rows, columns);
        final Index<ZonedDateTime> rowKeys = Index.of(rows.keys().filter(d -> d.isAfter(first) && d.isBefore(last)).collect(Collectors.toList()));
        final DataFrame<ZonedDateTime,String> slice = frame.rows().select(row -> rowKeys.contains(row.key()));
        if      (type == boolean.class)     frame.rows().filter(row -> rowKeys.contains(row.key())).forEach(row -> row.applyBooleans(v -> true));
        else if (type == int.class)         frame.rows().filter(row -> rowKeys.contains(row.key())).forEach(row -> row.applyInts(v -> 125));
        else if (type == long.class)        frame.rows().filter(row -> rowKeys.contains(row.key())).forEach(row -> row.applyLongs(v -> 2000L));
        else if (type == double.class)      frame.rows().filter(row -> rowKeys.contains(row.key())).forEach(row -> row.applyDoubles(v -> 0.456d));
        else if (type == Object.class)      frame.rows().filter(row -> rowKeys.contains(row.key())).forEach(row -> row.applyValues(v -> "0.456d"));
        else                                throw new Exception("Unsupported type " + type);
        final DataFrame<ZonedDateTime,String> expected = TestDataFrames.random(type, rowKeys, columns);
        expected.applyValues(v -> frame.data().getValue(v.rowKey(), v.colKey()));
        DataFrameAsserts.assertEqualsByIndex(expected, slice);
    }


    @Test(dataProvider="frameTypes")
    public void testSelectionUpdatesModifyUnderlying(Class type) throws Exception {
        final ZonedDateTime first = startTime.plusDays(14);
        final ZonedDateTime last = startTime.plusDays(28);
        final DataFrame<ZonedDateTime,String> frame = TestDataFrames.random(type, rows, columns);
        final Index<ZonedDateTime> rowKeys = Index.of(rows.keys().filter(d -> d.isAfter(first) && d.isBefore(last)).collect(Collectors.toList()));
        final DataFrame<ZonedDateTime,String> slice = frame.rows().select(row -> rowKeys.contains(row.key()));
        if      (type == boolean.class)     slice.rows().forEach(row -> row.applyBooleans(v -> true));
        else if (type == int.class)         slice.rows().forEach(row -> row.applyInts(v -> 125));
        else if (type == long.class)        slice.rows().forEach(row -> row.applyLongs(v -> 2000L));
        else if (type == double.class)      slice.rows().forEach(row -> row.applyDoubles(v -> 0.456d));
        else if (type == Object.class)      slice.rows().forEach(row -> row.applyValues(v -> "0.456d"));
        else                                throw new Exception("Unsupported type " + type);
        final DataFrame<ZonedDateTime,String> expected = TestDataFrames.random(type, rowKeys, columns);
        expected.applyValues(v -> frame.data().getValue(v.rowKey(), v.colKey()));
        DataFrameAsserts.assertEqualsByIndex(expected, slice);
    }


    @Test(dataProvider="frameTypes", expectedExceptions=DataFrameException.class)
    public void testStructureChangeOnSelectionFails(Class type) {
        final ZonedDateTime first = startTime.plusDays(14);
        final ZonedDateTime last = startTime.plusDays(28);
        final DataFrame<ZonedDateTime,String> frame = TestDataFrames.random(type, rows, columns);
        final DataFrame<ZonedDateTime,String> slice = frame.rows().select(row -> row.key().isAfter(first) && row.key().isBefore(last));
        slice.rows().add(ZonedDateTime.now());
    }


}
