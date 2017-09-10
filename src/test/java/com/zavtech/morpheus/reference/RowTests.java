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

import java.io.IOException;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.PrimitiveIterator;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayType;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameAsserts;
import com.zavtech.morpheus.frame.DataFrameRow;
import com.zavtech.morpheus.frame.DataFrameRows;
import com.zavtech.morpheus.frame.DataFrameValue;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.stats.StatType;
import com.zavtech.morpheus.stats.Stats;
import com.zavtech.morpheus.util.PerfStat;

/**
 * Tests of the DataFrameRows interface
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class RowTests {


    @DataProvider(name="args3")
    public Object[][] args3() {
        return new Object[][] {
            { false },
            { true },
        };
    }


    @DataProvider(name="args1")
    public Object[][] args1() {
        return new Object[][] {
            { boolean.class },
            { int.class },
            { long.class },
            { double.class },
            { String.class },
        };
    }


    @DataProvider(name="args2")
    public Object[][] args2() {
        return new Object[][] {
            { boolean.class, false },
            { boolean.class, true },
            { int.class, false },
            { int.class, true },
            { long.class, false },
            { long.class, true },
            { double.class, false },
            { double.class, true },
            { String.class, false },
            { String.class, true },
            { boolean.class, false },
            { boolean.class, true },
            { int.class, false },
            { int.class, true },
            { long.class, false },
            { long.class, true },
            { double.class, false },
            { double.class, true },
            { String.class, false },
            { String.class, true },
        };
    }


    @Test()
    public void testFirstAndLastRows() {
        final DataFrame<String,String> frame = DataFrame.of(String.class, String.class);
        Assert.assertTrue(!frame.rows().first().isPresent(), "No first row");
        Assert.assertTrue(!frame.rows().last().isPresent(), "No last row");
        frame.update(TestDataFrames.random(double.class, 10, 10), true, true);
        Assert.assertEquals(frame.rowCount(), 10, "Row count is 10");
        Assert.assertEquals(frame.colCount(), 10, "Column count is 10");
        Assert.assertTrue(frame.rows().first().isPresent(), "Now there is a first row");
        Assert.assertTrue(frame.rows().last().isPresent(), "Now there is a last row");
        frame.rows().first().get().forEachValue(v -> Assert.assertEquals(v.getDouble(), frame.data().getDouble(v.rowOrdinal(), v.colOrdinal())));
        frame.rows().last().get().forEachValue(v -> Assert.assertEquals(v.getDouble(), frame.data().getDouble(v.rowOrdinal(), v.colOrdinal())));
    }


    @Test(dataProvider= "args2")
    public void testForEachRow(Class type , boolean parallel) throws Exception {
        final DataFrame<String,String> frame = TestDataFrames.random(type, 500000, 10);
        final DataFrameRows<String,String> rows = parallel ? frame.rows().parallel() : frame.rows().sequential();
        rows.forEach(row -> row.forEachValue(value -> {
            final String rowKey = value.rowKey();
            final String colKey = value.colKey();
            final int rowIndex = frame.rows().ordinalOf(rowKey);
            final int colOrdinal = frame.cols().ordinalOf(colKey);
            switch (ArrayType.of(type)) {
                case BOOLEAN:
                    final boolean actualBoolean = value.getBoolean();
                    final boolean expectedBoolean = frame.data().getBoolean(rowIndex, colOrdinal);
                    assertEquals("The row indexes match", rowIndex, value.rowOrdinal());
                    assertEquals("The column indexes match", colOrdinal, value.colOrdinal());
                    assertEquals("The row keys match", frame.rows().key(rowIndex), rowKey);
                    assertEquals("The column keys match", frame.cols().key(colOrdinal), colKey);
                    assertEquals("Iterator values match for coordinates (" + rowIndex + "," + colOrdinal + ")", expectedBoolean, actualBoolean);
                    break;
                case INTEGER:
                    final int actualInt = value.getInt();
                    final int expectedInt = frame.data().getInt(rowIndex, colOrdinal);
                    assertEquals("The row indexes match", rowIndex, value.rowOrdinal());
                    assertEquals("The column indexes match", colOrdinal, value.colOrdinal());
                    assertEquals("The row keys match", frame.rows().key(rowIndex), rowKey);
                    assertEquals("The column keys match", frame.cols().key(colOrdinal), colKey);
                    assertEquals("Iterator values match for coordinates (" + rowIndex + "," + colOrdinal + ")", expectedInt, actualInt);
                    break;
                case LONG:
                    final long actualLong = value.getLong();
                    final long expectedLong = frame.data().getLong(rowIndex, colOrdinal);
                    assertEquals("The row indexes match", rowIndex, value.rowOrdinal());
                    assertEquals("The column indexes match", colOrdinal, value.colOrdinal());
                    assertEquals("The row keys match", frame.rows().key(rowIndex), rowKey);
                    assertEquals("The column keys match", frame.cols().key(colOrdinal), colKey);
                    assertEquals("Iterator values match for coordinates (" + rowIndex + "," + colOrdinal + ")", expectedLong, actualLong);
                    break;
                case DOUBLE:
                    final double actualDouble = value.getDouble();
                    final double expectedDouble = frame.data().getDouble(rowIndex, colOrdinal);
                    assertEquals("The row indexes match", rowIndex, value.rowOrdinal());
                    assertEquals("The column indexes match", colOrdinal, value.colOrdinal());
                    assertEquals("The row keys match", frame.rows().key(rowIndex), rowKey);
                    assertEquals("The column keys match", frame.cols().key(colOrdinal), colKey);
                    assertEquals("Iterator values match for coordinates (" + rowIndex + "," + colOrdinal + ")", expectedDouble, actualDouble);
                    break;
                case STRING:
                    final Object actualString = value.getValue();
                    final Object expectedString = frame.data().getValue(rowIndex, colOrdinal);
                    assertEquals("The row indexes match", rowIndex, value.rowOrdinal());
                    assertEquals("The column indexes match", colOrdinal, value.colOrdinal());
                    assertEquals("The row keys match", frame.rows().key(rowIndex), rowKey);
                    assertEquals("The column keys match", frame.cols().key(colOrdinal), colKey);
                    assertEquals("Iterator values match for coordinates (" + rowIndex + "," + colOrdinal + ")", expectedString, actualString);
                    break;
                case OBJECT:
                    final Object actualValue = value.getValue();
                    final Object expectedValue = frame.data().getValue(rowIndex, colOrdinal);
                    assertEquals("The row indexes match", rowIndex, value.rowOrdinal());
                    assertEquals("The column indexes match", colOrdinal, value.colOrdinal());
                    assertEquals("The row keys match", frame.rows().key(rowIndex), rowKey);
                    assertEquals("The column keys match", frame.cols().key(colOrdinal), colKey);
                    assertEquals("Iterator values match for coordinates (" + rowIndex + "," + colOrdinal + ")", expectedValue, actualValue);
                    break;
            }
        }));
    }


    @Test(dataProvider= "args2")
    public void testForEachValue(Class type , boolean parallel) {
        final DataFrame<String,String> frame = TestDataFrames.random(type, 500000, 10);
        final DataFrameRows<String,String> rows = parallel ? frame.rows().parallel() : frame.rows().sequential();
        rows.keys().forEach(key -> frame.rowAt(key).forEachValue(value -> {
            final String rowKey = value.rowKey();
            final String colKey = value.colKey();
            final int rowIndex = value.rowOrdinal();
            final int colOrdinal = value.colOrdinal();
            switch (ArrayType.of(type)) {
                case BOOLEAN:
                    final boolean actualBoolean = value.getBoolean();
                    final boolean expectedBoolean = frame.data().getBoolean(rowIndex, colOrdinal);
                    assertEquals("The row indexes match", rowIndex, value.rowOrdinal());
                    assertEquals("The column indexes match", colOrdinal, value.colOrdinal());
                    assertEquals("The row keys match", frame.rows().key(rowIndex), rowKey);
                    assertEquals("The column keys match", frame.cols().key(colOrdinal), colKey);
                    assertEquals("Iterator values match for coordinates (" + rowIndex + "," + colOrdinal + ")", expectedBoolean, actualBoolean);
                    break;
                case INTEGER:
                    final int actualInt = value.getInt();
                    final int expectedInt = frame.data().getInt(rowIndex, colOrdinal);
                    assertEquals("The row indexes match", rowIndex, value.rowOrdinal());
                    assertEquals("The column indexes match", colOrdinal, value.colOrdinal());
                    assertEquals("The row keys match", frame.rows().key(rowIndex), rowKey);
                    assertEquals("The column keys match", frame.cols().key(colOrdinal), colKey);
                    assertEquals("Iterator values match for coordinates (" + rowIndex + "," + colOrdinal + ")", expectedInt, actualInt);
                    break;
                case LONG:
                    final long actualLong = value.getLong();
                    final long expectedLong = frame.data().getLong(rowIndex, colOrdinal);
                    assertEquals("The row indexes match", rowIndex, value.rowOrdinal());
                    assertEquals("The column indexes match", colOrdinal, value.colOrdinal());
                    assertEquals("The row keys match", frame.rows().key(rowIndex), rowKey);
                    assertEquals("The column keys match", frame.cols().key(colOrdinal), colKey);
                    assertEquals("Iterator values match for coordinates (" + rowIndex + "," + colOrdinal + ")", expectedLong, actualLong);
                    break;
                case DOUBLE:
                    final double actualDouble = value.getDouble();
                    final double expectedDouble = frame.data().getDouble(rowIndex, colOrdinal);
                    assertEquals("The row indexes match", rowIndex, value.rowOrdinal());
                    assertEquals("The column indexes match", colOrdinal, value.colOrdinal());
                    assertEquals("The row keys match", frame.rows().key(rowIndex), rowKey);
                    assertEquals("The column keys match", frame.cols().key(colOrdinal), colKey);
                    assertEquals("Iterator values match for coordinates (" + rowIndex + "," + colOrdinal + ")", expectedDouble, actualDouble);
                    break;
                case OBJECT:
                    final Object actualValue = value.getValue();
                    final Object expectedValue = frame.data().getValue(rowIndex, colOrdinal);
                    assertEquals("The row indexes match", rowIndex, value.rowOrdinal());
                    assertEquals("The column indexes match", colOrdinal, value.colOrdinal());
                    assertEquals("The row keys match", frame.rows().key(rowIndex), rowKey);
                    assertEquals("The column keys match", frame.cols().key(colOrdinal), colKey);
                    assertEquals("Iterator values match for coordinates (" + rowIndex + "," + colOrdinal + ")", expectedValue, actualValue);
                    break;
            }
        }));
    }


    @Test()
    public void testForEachValueParallel() {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 1, 1000000);
        final String rowKey = frame.rows().key(0);
        frame.applyDoubles(v -> Math.random() * 100);
        frame.parallel().rowAt(rowKey).forEachValue(v -> {
            final double actual = v.getDouble();
            final double expected = frame.data().getDouble(v.rowKey(), v.colKey());
            Assert.assertEquals(actual, expected, "Values match for " + v);
        });
    }


    @Test()
    public void testFilter1() {
        final LocalDate start = LocalDate.of(2000, 1, 1);
        final Index<LocalDate> dates = Range.of(0, 2000).map(start::plusDays).toIndex(LocalDate.class);
        final Index<String> columns = Range.of(0, 100).map(i -> "Column-" +i).toIndex(String.class);
        final DataFrame<LocalDate,String> frame = DataFrame.of(dates, columns, Double.class).applyDoubles(v -> Math.random() * 100);
        final Array<LocalDate> selectionKeys = Array.of(LocalDate.class, dates.getKey(0), dates.getKey(25), dates.getKey(102), dates.getKey(456));
        final DataFrameRows<LocalDate,String> filter = frame.rows().filter(selectionKeys);
        Assert.assertEquals(filter.count(), 4, "Row count matches expected row count");
        filter.forEach(row -> row.forEachValue(v -> {
            final double actual = v.getDouble();
            final double expected = frame.data().getDouble(v.rowKey(), v.colKey());
            if (actual != expected) {
                System.out.println("Break");
            }
            Assert.assertEquals(actual, expected, "The value matches for " + v);
        }));
    }


    @Test(dataProvider= "args3")
    public void testFilter2(boolean parallel) {
        final LocalDate start = LocalDate.of(2000, 1, 1);
        final Index<LocalDate> dates = Range.of(0, 2000).map(start::plusDays).toIndex(LocalDate.class);
        final Index<String> columns = Range.of(0, 100).map(i -> "Column-" +i).toIndex(String.class);
        final DataFrame<LocalDate,String> frame = DataFrame.of(dates, columns, Double.class);
        frame.applyDoubles(v -> Math.random() * 100);
        final DataFrameRows<LocalDate,String> rows = parallel ? frame.rows().parallel() : frame.rows().sequential();
        final DataFrameRows<LocalDate,String> filter = rows.filter(row -> row.key().getDayOfWeek() == DayOfWeek.MONDAY);
        final long expectedCount = dates.keys().filter(d -> d.getDayOfWeek() == DayOfWeek.MONDAY).count();
        Assert.assertEquals(filter.count(), expectedCount, "Row count matches expected number of mondays");
        filter.forEach(row -> Assert.assertEquals(DayOfWeek.MONDAY, row.key().getDayOfWeek()));
    }


    @Test()
    public void testIntStream() throws Exception {
        final DataFrame<String,String> frame = TestDataFrames.random(int.class, 100, 100);
        frame.rows().forEach(row -> {
            final int[] rowValues = row.toIntStream().toArray();
            for (int i = 0; i < row.size(); ++i) {
                final String rowKey = row.key();
                final String colKey = frame.cols().key(i);
                final int actual = rowValues[i];
                final int expected = row.getInt(i);
                Assert.assertEquals(actual, expected, "Row values match at coordinates (" + rowKey + "," + colKey + ")");
            }
        });
    }


    @Test()
    public void testLongStream() throws Exception {
        final DataFrame<String,String> frame = TestDataFrames.random(long.class, 100, 100);
        frame.rows().forEach(row -> {
            final long[] rowValues = row.toLongStream().toArray();
            for (int i=0; i<row.size(); ++i) {
                final String rowKey = row.key();
                final String colKey = frame.cols().key(i);
                final long actual = rowValues[i];
                final long expected = row.getLong(i);
                Assert.assertEquals(actual, expected, "Row values match at coordinates (" + rowKey + "," + colKey + ")");
            }
        });
    }


    @Test()
    public void testDoubleStream() throws Exception {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        frame.rows().forEach(row -> {
            final double[] rowValues = row.toDoubleStream().toArray();
            for (int i=0; i<row.size(); ++i) {
                final String rowKey = row.key();
                final String colKey = frame.cols().key(i);
                final double actual = rowValues[i];
                final double expected = row.getDouble(i);
                Assert.assertEquals(actual, expected, "Row values match at coordinates (" + rowKey + "," + colKey + ")");
            }
        });
    }


    @Test()
    public void testValueStream() throws Exception {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        frame.rows().forEach(row -> {
            final Object[] rowValues = row.toValueStream().toArray();
            for (int i=0; i<row.size(); ++i) {
                final String rowKey = row.key();
                final String colKey = frame.cols().key(i);
                final Object actual = rowValues[i];
                final Object expected = row.getValue(i);
                Assert.assertEquals(actual, expected, "Row values match at coordinates (" + rowKey + "," + colKey + ")");
            }
        });
    }


    @Test(dataProvider= "args1")
    public void testRowIterator(Class type) {
        final DataFrame<String,String> frame = TestDataFrames.random(type, 20, 20);
        frame.rows().forEach(row -> {
            int count = 0;
            final Iterator<Object> iterator1 = row.toValueStream().iterator();
            final Iterator<DataFrameValue<String,String>> iterator2 = row.iterator();
            while (iterator1.hasNext()) {
                ++count;
                final Object v1 = iterator1.next();
                final DataFrameValue<String,String> value = iterator2.next();
                final Object v2 = value.getValue();
                Assert.assertEquals(v2, v1, "Value match at " + value.rowKey() + ", " + value.colKey());
            }
            final int colCount = row.size();
            Assert.assertEquals(colCount, count, "The column count matches");
        });
    }


    @Test()
    public void testRowIteratorWithPredicate() throws IOException {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 20, 20);
        frame.applyValues(v -> Math.random() * 10);
        frame.colAt("C5").applyDoubles(v -> Double.NaN);
        frame.colAt("C10").applyDoubles(v -> Double.NaN);
        frame.rows().forEach(row -> {
            int count = 0;
            final PrimitiveIterator.OfDouble iterator1 = row.toDoubleStream().filter(v -> !Double.isNaN(v)).iterator();
            final Iterator<DataFrameValue<String,String>> iterator2 = row.iterator(v -> !Double.isNaN(v.getDouble()));
            while (iterator1.hasNext()) {
                ++count;
                final double v1 = iterator1.next();
                Assert.assertTrue(iterator2.hasNext(), "Row iterator has next");
                final DataFrameValue<String,String> value = iterator2.next();
                final double v2 = value.getDouble();
                Assert.assertEquals(v2, v1, "Value match at " + value.rowKey() + ", " + value.colKey());
            }
            final int colCount = row.size()-2;
            Assert.assertEquals(colCount, count, "The column count matches");
        });
    }


    @Test(dataProvider= "args1")
    public void testRowToDataFrame(Class type) throws Exception {
        final DataFrame<String,String> frame = TestDataFrames.random(type, 10, 10);
        final DataFrameRow<String,String> row = frame.rowAt("R7");
        if (type == boolean.class) {
            row.forEachValue(v -> Assert.assertEquals(v.getBoolean(), frame.data().getBoolean(v.rowKey(), v.colKey())));
            final DataFrame<String,String> columnFrame = row.toDataFrame();
            row.forEachValue(v -> Assert.assertEquals(v.getBoolean(), columnFrame.data().getBoolean(v.rowKey(), v.colKey())));
        } else if (type == int.class) {
            row.forEachValue(v -> Assert.assertEquals(v.getInt(), frame.data().getInt(v.rowKey(), v.colKey())));
            final DataFrame<String,String> columnFrame = row.toDataFrame();
            row.forEachValue(v -> Assert.assertEquals(v.getInt(), columnFrame.data().getInt(v.rowKey(), v.colKey())));
        } else if (type == long.class) {
            row.forEachValue(v -> Assert.assertEquals(v.getLong(), frame.data().getLong(v.rowKey(), v.colKey())));
            final DataFrame<String,String> columnFrame = row.toDataFrame();
            row.forEachValue(v -> Assert.assertEquals(v.getLong(), columnFrame.data().getLong(v.rowKey(), v.colKey())));
        } else if (type == double.class) {
            row.forEachValue(v -> Assert.assertEquals(v.getDouble(), frame.data().getDouble(v.rowKey(), v.colKey())));
            final DataFrame<String,String> columnFrame = row.toDataFrame();
            row.forEachValue(v -> Assert.assertEquals(v.getDouble(), columnFrame.data().getDouble(v.rowKey(), v.colKey())));
        } else if (type == String.class) {
            row.forEachValue(v -> Assert.assertEquals(v.getValue().toString(), frame.data().getValue(v.rowKey(), v.colKey()).toString()));
            final DataFrame<String,String> columnFrame = row.toDataFrame();
            row.forEachValue(v -> Assert.assertEquals(v.getValue().toString(), columnFrame.data().getValue(v.rowKey(), v.colKey()).toString()));
        } else {
            throw new Exception("Unsupported type: " + type);
        }
    }


    @Test(dataProvider= "args1")
    public void testRowMapping1(Class type) {
        final DataFrame<String,String> frame = TestDataFrames.random(type, 100, 100);
        frame.rows().forEach(row -> {
            if (type == boolean.class) row.applyBooleans(v -> v.rowOrdinal() % 2 == 0);
            else if (type == int.class) row.applyInts(v -> v.rowOrdinal() * 10);
            else if (type == long.class) row.applyLongs(v -> v.rowOrdinal() * 10);
            else if (type == double.class) row.applyDoubles(v -> v.rowOrdinal() * 10d);
            else if (type == String.class) row.applyValues(v -> v.rowOrdinal() + "," + v.colOrdinal());
            else if (type == Object.class) row.applyValues(v -> v.rowOrdinal() + "," + v.colOrdinal());
            else throw new IllegalArgumentException("Unexpected type: " + type);
        });
        for (int rowIndex=0; rowIndex<frame.rowCount(); ++rowIndex) {
            for (int colOrdinal = 0; colOrdinal<frame.colCount(); ++colOrdinal) {
                if (type == boolean.class)  Assert.assertEquals(frame.data().getBoolean(rowIndex, colOrdinal), rowIndex % 2 == 0, "Match at " + rowIndex + ", " + colOrdinal);
                if (type == int.class)      Assert.assertEquals(frame.data().getInt(rowIndex, colOrdinal), rowIndex * 10, "Match at " + rowIndex + ", " + colOrdinal);
                if (type == long.class)     Assert.assertEquals(frame.data().getLong(rowIndex, colOrdinal), rowIndex * 10L, "Match at " + rowIndex + ", " + colOrdinal);
                if (type == double.class)   Assert.assertEquals(frame.data().getDouble(rowIndex, colOrdinal), rowIndex * 10d, "Match at " + rowIndex + ", " + colOrdinal);
                if (type == String.class)   Assert.assertEquals(frame.data().getValue(rowIndex, colOrdinal), rowIndex + "," + colOrdinal, "Match at " + rowIndex + ", " + colOrdinal);
                if (type == Object.class)   Assert.assertEquals(frame.data().getValue(rowIndex, colOrdinal), rowIndex + "," + colOrdinal, "Match at " + rowIndex + ", " + colOrdinal);
            }
        }
    }


    @Test(dataProvider= "args1")
    public void testRowMapping2(Class type) {
        final Index<String> keys = Index.of("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");
        final DataFrame<String,String> source = TestDataFrames.random(type, keys, keys);
        final DataFrame<String,String> target = TestDataFrames.random(type, keys, keys);
        if (type == boolean.class) {
            target.rows().keys().forEach(rowKey -> target.rowAt(rowKey).applyBooleans(v -> source.data().getBoolean(rowKey, v.colKey())));
            DataFrameAsserts.assertEqualsByIndex(source, target);
        } else if (type == int.class) {
            target.rows().keys().forEach(rowKey -> target.rowAt(rowKey).applyInts(v -> source.data().getInt(rowKey, v.colKey())));
            DataFrameAsserts.assertEqualsByIndex(source, target);
        } else if (type == long.class) {
            target.rows().keys().forEach(rowKey -> target.rowAt(rowKey).applyLongs(v -> source.data().getLong(rowKey, v.colKey())));
            DataFrameAsserts.assertEqualsByIndex(source, target);
        } else if (type == double.class) {
            target.rows().keys().forEach(rowKey -> target.rowAt(rowKey).applyDoubles(v -> source.data().getDouble(rowKey, v.colKey())));
            DataFrameAsserts.assertEqualsByIndex(source, target);
        } else {
            target.rows().keys().forEach(rowKey -> target.rowAt(rowKey).applyValues(v -> source.data().getValue(rowKey, v.colKey())));
            DataFrameAsserts.assertEqualsByIndex(source, target);
        }
    }


    @Test()
    public void testFindFirstNoMatch() {
        final List<String> keyList = new ArrayList<>();
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        final Optional<DataFrameRow<String,String>> rowMatch = frame.rows().first(row -> {
            keyList.add(row.key());
            return false;
        });
        Assert.assertTrue(!rowMatch.isPresent(), "No match was found");
        Assert.assertEquals(keyList.size(), frame.rowCount(), "All rows were checked");
    }


    @Test()
    public void testFindLastNoMatch() {
        final List<String> keyList = new ArrayList<>();
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        final Optional<DataFrameRow<String,String>> rowMatch = frame.rows().last(row -> {
            keyList.add(row.key());
            return false;
        });
        Assert.assertTrue(!rowMatch.isPresent(), "No match was found");
        Assert.assertEquals(keyList.size(), frame.rowCount(), "All rows were checked");
    }


    @Test()
    public void testFindFirstWithMatch() {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        final String rowKeyToFind = frame.rows().key(85);
        final DataFrameRow<String,String> rowToFind = frame.rowAt(rowKeyToFind);
        final List<String> rowKeys = new ArrayList<>();
        final Optional<DataFrameRow<String,String>> rowMatch = frame.rows().first(row -> {
            rowKeys.add(row.key());
            return rowToFind.equals(row);
        });
        Assert.assertTrue(rowMatch.isPresent(), "Found a matching row");
        Assert.assertEquals(rowMatch.get().key(), rowKeyToFind, "The row keys match");
        for (int i=0; i<rowKeys.size(); ++i) {
            final String expected = frame.rows().key(i);
            final String actual = rowKeys.get(i);
            Assert.assertEquals(actual, expected, "The keys match");
        }
    }


    @Test()
    public void testFindLastWithMatch() {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        final String rowKeyToFind = frame.rows().key(85);
        final DataFrameRow<String,String> rowToFind = frame.rowAt(rowKeyToFind);
        final List<String> rowKeys = new ArrayList<>();
        final Optional<DataFrameRow<String,String>> rowMatch = frame.rows().last(row -> {
            rowKeys.add(row.key());
            if (row.key().equals(rowKeyToFind)) {
                return rowToFind.equals(row);
            }
            return rowToFind.equals(row);
        });
        Assert.assertTrue(rowMatch.isPresent(), "Found a matching row");
        Assert.assertEquals(rowMatch.get().key(), rowKeyToFind, "The row keys match");
        for (int i=0; i<rowKeys.size(); ++i) {
            final String expected = frame.rows().key(frame.rowCount()-1-i);
            final String actual = rowKeys.get(i);
            Assert.assertEquals(actual, expected, "The keys match");
        }
    }


    @Test()
    public void testRowStats() {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        final Stats<Double> stats1 = frame.rowAt("R7").stats();
        final Stats<Double> stats2 = frame.rowAt("R7").toDataFrame().stats();
        for (StatType stat : StatType.univariate()) {
            final double v1 = stat.apply(stats1);
            final double v2 = stat.apply(stats2);
            Assert.assertEquals(v1, v2, "The stat for " + stat.name() + " matches");
        }
    }


    @Test()
    public void testSelectWithParallelProcessing() {
        final ZonedDateTime start = ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneId.of("GMT"));
        final Index<ZonedDateTime> timestamps = Range.of(0, 5000000).map(start::plusMinutes).toIndex(ZonedDateTime.class);
        final DataFrame<ZonedDateTime,String> frame = DataFrame.of(timestamps, "C", Double.class);
        frame.applyDoubles(v -> Math.random() * 100);
        final long expectedRowCount = timestamps.keys().filter(t -> t.getDayOfWeek() == DayOfWeek.MONDAY).count();
        final long t1 = System.nanoTime();
        final DataFrame<ZonedDateTime,String> select1 = frame.rows().parallel().select(row -> row.key().getDayOfWeek() == DayOfWeek.MONDAY);
        final long t2 = System.nanoTime();
        final DataFrame<ZonedDateTime,String> select2 = frame.rows().sequential().select(row -> row.key().getDayOfWeek() == DayOfWeek.MONDAY);
        final long t3 = System.nanoTime();
        System.out.println("Sequential select in " + ((t3-t2)/1000000) + " millis, parallel select in " + ((t2-t1)/1000000) + " millis");
        Assert.assertEquals(select1.rowCount(), expectedRowCount, "Selection 1 has expected row count");
        Assert.assertEquals(select2.rowCount(), expectedRowCount, "Selection 1 has expected row count");
        Assert.assertEquals(select1.colCount(), 1, "Selection 1 has expected column count");
        Assert.assertEquals(select2.colCount(), 1, "Selection 1 has expected column count");
        DataFrameAsserts.assertEqualsByIndex(select1, select2);
    }


    @Test()
    public void testStreamOfRows() {
        final int[] rowCount = new int[1];
        final LocalDate start = LocalDate.now().minusYears(10);
        final Index<String> columns = Index.of("X", "Y", "Z");
        final Index<LocalDate> dates = Range.of(0, 5000).map(start::plusDays).toIndex(LocalDate.class);
        final DataFrame<LocalDate,String> frame = DataFrame.of(dates, columns, Double.class);
        frame.applyDoubles(v -> Math.random() * 100);
        frame.rows().stream().forEach(row -> {
            rowCount[0]++;
            final LocalDate date = row.key();
            Assert.assertEquals(row.size(), frame.colCount(), "Column count matched");
            final DataFrameRow<LocalDate,String> expected = frame.rowAt(date);
            for (int i=0; i<frame.cols().count(); ++i) {
                final double v1 = row.getDouble(i);
                final double v2 = expected.getDouble(i);
                Assert.assertEquals(v1, v2, "The values match for row: " + date);
                Assert.assertEquals(row.stats().mean(), expected.stats().mean(), "Mean matches");
            }
        });
        Assert.assertEquals(rowCount[0], frame.rowCount(), "Processed expected number of rows");
    }


    @Test()
    public void testIteratorOfRows() {
        int rowCount = 0;
        final LocalDate start = LocalDate.now().minusYears(10);
        final Index<LocalDate> dates = Range.of(0, 5000).map(start::plusDays).toIndex(LocalDate.class);
        final DataFrame<LocalDate,String> frame = DataFrame.of(dates, Index.of("X", "Y", "Z"), Double.class);
        frame.applyDoubles(v -> Math.random() * 100);
        final Iterator<DataFrameRow<LocalDate,String>> iterator = frame.rows().iterator();
        while (iterator.hasNext()) {
            rowCount++;
            final DataFrameRow<LocalDate,String> row = iterator.next();
            Assert.assertEquals(row.size(), frame.colCount(), "Column count matched");
            final LocalDate date = row.key();
            final DataFrameRow<LocalDate,String> expected = frame.rowAt(date);
            for (int i=0; i<frame.cols().count(); ++i) {
                final double v1 = row.getDouble(i);
                final double v2 = expected.getDouble(i);
                Assert.assertEquals(v1, v2, "The values match for row: " + date);
                Assert.assertEquals(row.stats().mean(), expected.stats().mean(), "Mean matches");
            }
        }
        Assert.assertEquals(rowCount, frame.rowCount(), "Processed expected number of rows");
    }


    @Test()
    public void testRowToDataFramePerformance() {
        final Range<Integer> rowKeys = Range.of(1, 10);
        final Range<Integer> colKeys = Range.of(1, 100000);
        final DataFrame<Integer,Integer> frame = DataFrame.ofDoubles(rowKeys, colKeys).applyDoubles(v -> Math.random());
        PerfStat.timeInMillis(100, () -> {
            final DataFrameRow<Integer,Integer> row = frame.rowAt(2);
            final DataFrame<Integer,Integer> rowFrame = row.toDataFrame();
            Assert.assertEquals(row.size(), frame.colCount());
            Assert.assertEquals(rowFrame.colCount(), frame.colCount());
            return rowFrame;
        }).print();
    }


    @Test()
    public void testRowToArray() {
        final DataFrame<Integer,String> frame = TestDataFrames.createMixedRandomFrame(Integer.class, 1000);
        frame.rows().forEach(row -> {
            final Array<Object> array = row.toArray();
            Assert.assertEquals(array.type(), Object.class, "Type is as expected");
            row.forEachValue(v -> {
                final Object expected = v.getValue();
                final Object actual = array.getValue(v.colOrdinal());
                Assert.assertEquals(actual, expected, "Values for column match for " + v.colKey());
            });
        });
    }

}
