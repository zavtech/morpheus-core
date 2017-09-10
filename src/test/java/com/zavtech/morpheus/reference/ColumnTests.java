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
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.PrimitiveIterator;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameAsserts;
import com.zavtech.morpheus.frame.DataFrameColumn;
import com.zavtech.morpheus.frame.DataFrameColumns;
import com.zavtech.morpheus.frame.DataFrameValue;
import com.zavtech.morpheus.array.ArrayType;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.stats.StatType;
import com.zavtech.morpheus.stats.Stats;
import com.zavtech.morpheus.util.PerfStat;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests of the DataFrameColumns interface
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class ColumnTests {


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
            { boolean.class, true,},
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
    public void testFirstAndLastColumns() {
        final DataFrame<String,String> frame = DataFrame.of(String.class, String.class);
        Assert.assertTrue(!frame.cols().first().isPresent(), "No first row");
        Assert.assertTrue(!frame.cols().last().isPresent(), "No last row");
        frame.update(TestDataFrames.random(double.class, 10, 10), true, true);
        Assert.assertEquals(frame.rowCount(), 10, "Row count is 10");
        Assert.assertEquals(frame.colCount(), 10, "Column count is 10");
        Assert.assertTrue(frame.cols().first().isPresent(), "Now there is a first row");
        Assert.assertTrue(frame.cols().last().isPresent(), "Now there is a last row");
        frame.cols().first().get().forEachValue(v -> Assert.assertEquals(v.getDouble(), frame.data().getDouble(v.rowOrdinal(), v.colOrdinal())));
        frame.cols().last().get().forEachValue(v -> Assert.assertEquals(v.getDouble(), frame.data().getDouble(v.rowOrdinal(), v.colOrdinal())));
    }


    @Test(dataProvider= "args2")
    public void testForEachColumn(Class type, boolean parallel) throws Exception {
        final DataFrame<Integer,Integer> frame = TestDataFrames.random2(type, 100000, 10);
        final DataFrameColumns<Integer,Integer> columns = parallel ? frame.cols().parallel() : frame.cols().sequential();
        columns.forEach(column -> {
            final ArrayType arrayType = ArrayType.of(column.typeInfo());
            column.forEachValue(value -> {
                final Integer rowKey = value.rowKey();
                final Integer colKey = value.colKey();
                final int rowOrdinal = frame.rows().ordinalOf(rowKey);
                final int colOrdinal = frame.cols().ordinalOf(colKey);
                switch (arrayType) {
                    case BOOLEAN:
                        final boolean actualBoolean = value.getBoolean();
                        final boolean expectedBoolean = frame.data().getBoolean(rowOrdinal, colOrdinal);
                        assertEquals("The row indexes match", rowOrdinal, value.rowOrdinal());
                        assertEquals("The column indexes match", colOrdinal, value.colOrdinal());
                        assertEquals("The row keys match", frame.rows().key(rowOrdinal), rowKey);
                        assertEquals("The column keys match", frame.cols().key(colOrdinal), colKey);
                        assertEquals("Iterator values match for coordinates (" + rowOrdinal + "," + colOrdinal + ")", expectedBoolean, actualBoolean);
                        break;
                    case INTEGER:
                        final int actualInt = value.getInt();
                        final int expectedInt = frame.data().getInt(rowOrdinal, colOrdinal);
                        assertEquals("The row indexes match", rowOrdinal, value.rowOrdinal());
                        assertEquals("The column indexes match", colOrdinal, value.colOrdinal());
                        assertEquals("The row keys match", frame.rows().key(rowOrdinal), rowKey);
                        assertEquals("The column keys match", frame.cols().key(colOrdinal), colKey);
                        assertEquals("Iterator values match for coordinates (" + rowOrdinal + "," + colOrdinal + ")", expectedInt, actualInt);
                        break;
                    case LONG:
                        final long actualLong = value.getLong();
                        final long expectedLong = frame.data().getLong(rowOrdinal, colOrdinal);
                        assertEquals("The row indexes match", rowOrdinal, value.rowOrdinal());
                        assertEquals("The column indexes match", colOrdinal, value.colOrdinal());
                        assertEquals("The row keys match", frame.rows().key(rowOrdinal), rowKey);
                        assertEquals("The column keys match", frame.cols().key(colOrdinal), colKey);
                        assertEquals("Iterator values match for coordinates (" + rowOrdinal + "," + colOrdinal + ")", expectedLong, actualLong);
                        break;
                    case DOUBLE:
                        final double actualDouble = value.getDouble();
                        final double expectedDouble = frame.data().getDouble(rowOrdinal, colOrdinal);
                        assertEquals("The row indexes match", rowOrdinal, value.rowOrdinal());
                        assertEquals("The column indexes match", colOrdinal, value.colOrdinal());
                        assertEquals("The row keys match", frame.rows().key(rowOrdinal), rowKey);
                        assertEquals("The column keys match", frame.cols().key(colOrdinal), colKey);
                        assertEquals("Iterator values match for coordinates (" + rowOrdinal + "," + colOrdinal + ")", expectedDouble, actualDouble);
                        break;
                    default:
                        final Object actualValue = value.getValue();
                        final Object expectedValue = frame.data().getValue(rowOrdinal, colOrdinal);
                        assertEquals("The row indexes match", rowOrdinal, value.rowOrdinal());
                        assertEquals("The column indexes match", colOrdinal, value.colOrdinal());
                        assertEquals("The row keys match", frame.rows().key(rowOrdinal), rowKey);
                        assertEquals("The column keys match", frame.cols().key(colOrdinal), colKey);
                        assertEquals("Iterator values match for coordinates (" + rowOrdinal + "," + colOrdinal + ")", expectedValue, actualValue);
                        break;
                }
            });
        });
    }


    @Test(dataProvider= "args2")
    public void testForEachValue(Class type, boolean parallel) {
        final DataFrame<String,String> frame = TestDataFrames.random(type, 10, 10000);
        final DataFrameColumns<String,String> columns = parallel ? frame.cols().parallel() : frame.cols().sequential();
        columns.keys().forEach(key -> frame.colAt(key).forEachValue(value -> {
            final String rowKey = value.rowKey();
            final String colKey = value.colKey();
            final int rowOrdinal = value.rowOrdinal();
            final int colOrdinal = value.colOrdinal();
            switch (ArrayType.of(type)) {
                case BOOLEAN:
                    final boolean actualBoolean = value.getBoolean();
                    final boolean expectedBoolean = frame.data().getBoolean(rowOrdinal, colOrdinal);
                    assertEquals("The row indexes match", rowOrdinal, value.rowOrdinal());
                    assertEquals("The column indexes match", colOrdinal, value.colOrdinal());
                    assertEquals("The row keys match", frame.rows().key(rowOrdinal), rowKey);
                    assertEquals("The column keys match", frame.cols().key(colOrdinal), colKey);
                    assertEquals("Iterator values match for coordinates (" + rowOrdinal + "," + colOrdinal + ")", expectedBoolean, actualBoolean);
                    break;
                case INTEGER:
                    final int actualInt = value.getInt();
                    final int expectedInt = frame.data().getInt(rowOrdinal, colOrdinal);
                    assertEquals("The row indexes match", rowOrdinal, value.rowOrdinal());
                    assertEquals("The column indexes match", colOrdinal, value.colOrdinal());
                    assertEquals("The row keys match", frame.rows().key(rowOrdinal), rowKey);
                    assertEquals("The column keys match", frame.cols().key(colOrdinal), colKey);
                    assertEquals("Iterator values match for coordinates (" + rowOrdinal + "," + colOrdinal + ")", expectedInt, actualInt);
                    break;
                case LONG:
                    final long actualLong = value.getLong();
                    final long expectedLong = frame.data().getLong(rowOrdinal, colOrdinal);
                    assertEquals("The row indexes match", rowOrdinal, value.rowOrdinal());
                    assertEquals("The column indexes match", colOrdinal, value.colOrdinal());
                    assertEquals("The row keys match", frame.rows().key(rowOrdinal), rowKey);
                    assertEquals("The column keys match", frame.cols().key(colOrdinal), colKey);
                    assertEquals("Iterator values match for coordinates (" + rowOrdinal + "," + colOrdinal + ")", expectedLong, actualLong);
                    break;
                case DOUBLE:
                    final double actualDouble = value.getDouble();
                    final double expectedDouble = frame.data().getDouble(rowOrdinal, colOrdinal);
                    assertEquals("The row indexes match", rowOrdinal, value.rowOrdinal());
                    assertEquals("The column indexes match", colOrdinal, value.colOrdinal());
                    assertEquals("The row keys match", frame.rows().key(rowOrdinal), rowKey);
                    assertEquals("The column keys match", frame.cols().key(colOrdinal), colKey);
                    assertEquals("Iterator values match for coordinates (" + rowOrdinal + "," + colOrdinal + ")", expectedDouble, actualDouble);
                    break;
                case OBJECT:
                    final Object actualValue = value.getValue();
                    final Object expectedValue = frame.data().getValue(rowOrdinal, colOrdinal);
                    assertEquals("The row indexes match", rowOrdinal, value.rowOrdinal());
                    assertEquals("The column indexes match", colOrdinal, value.colOrdinal());
                    assertEquals("The row keys match", frame.rows().key(rowOrdinal), rowKey);
                    assertEquals("The column keys match", frame.cols().key(colOrdinal), colKey);
                    assertEquals("Iterator values match for coordinates (" + rowOrdinal + "," + colOrdinal + ")", expectedValue, actualValue);
                    break;
            }
        }));
    }


    @Test()
    public void testForEachValueParallel() {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 1000000, 1);
        final String colKey = frame.cols().key(0);
        frame.applyDoubles(v -> Math.random() * 100);
        frame.parallel().colAt(colKey).forEachValue(v -> {
            final double actual = v.getDouble();
            final double expected = frame.data().getDouble(v.rowKey(), v.colKey());
            Assert.assertEquals(actual, expected, "Values match for " + v);
        });
    }


    @Test()
    public void testFilter1() {
        final LocalDate start = LocalDate.of(2000, 1, 1);
        final LocalDate end = start.plusYears(10);
        final Index<LocalDate> dates = Range.of(start, end).toIndex(LocalDate.class);
        final Index<String> strings = Range.of(0, 100).map(i -> "Column-" +i).toIndex(String.class);
        final DataFrame<String,LocalDate> frame = DataFrame.ofDoubles(strings, dates);
        frame.applyDoubles(v -> Math.random() * 100);
        final DataFrameColumns<String,LocalDate> filter = frame.cols().filter(Arrays.asList(dates.getKey(0), dates.getKey(25), dates.getKey(102), dates.getKey(456)));
        Assert.assertEquals(filter.count(), 4, "Column count matches expected column count");
        filter.forEach(column -> column.forEachValue(v -> {
            final double actual = v.getDouble();
            final double expected = frame.data().getDouble(v.rowKey(), v.colKey());
            Assert.assertEquals(actual, expected, "The value matches for " + v);
        }));
    }


    @Test(dataProvider= "args3")
    public void testFilter2(boolean parallel) {
        final LocalDate start = LocalDate.of(2000, 1, 1);
        final LocalDate end = start.plusDays(2000);
        final Range<LocalDate> dates = Range.of(start, end);
        final Range<String> labels = Range.of(0, 100).map(i -> "Column" + i);
        final DataFrame<String,LocalDate> frame = DataFrame.ofDoubles(labels, dates).applyDoubles(v -> Math.random() * 100);
        final DataFrameColumns<String,LocalDate> cols = parallel ? frame.cols().parallel() : frame.cols().sequential();
        final DataFrameColumns<String,LocalDate> filter = cols.filter(col -> col.key().getDayOfWeek() == DayOfWeek.MONDAY);
        final long expectedCount = dates.stream().filter(d -> d.getDayOfWeek() == DayOfWeek.MONDAY).count();
        Assert.assertEquals(filter.count(), expectedCount, "Row count matches expected number of mondays");
        filter.forEach(row -> Assert.assertEquals(DayOfWeek.MONDAY, row.key().getDayOfWeek()));
    }


    @Test()
    public void testIntStream() throws Exception {
        final DataFrame<String,String> frame = TestDataFrames.random(int.class, 100, 100);
        frame.cols().forEach(column -> {
            final int[] columnValues = column.values().mapToInt(DataFrameValue::getInt).toArray();
            for (int i = 0; i < column.size(); ++i) {
                final String rowKey = frame.rows().key(i);
                final String colKey = column.key();
                final int actual = columnValues[i];
                final int expected = column.getInt(i);
                Assert.assertEquals(actual, expected, "Column values match at coordinates (" + rowKey + "," + colKey + ")");
            }
        });
    }


    @Test()
    public void testLongStream() throws Exception {
        final DataFrame<String,String> frame = TestDataFrames.random(long.class, 100, 100);
        frame.cols().forEach(column -> {
            final long[] columnValues = column.values().mapToLong(DataFrameValue::getLong).toArray();
            for (int i=0; i<column.size(); ++i) {
                final String rowKey = frame.rows().key(i);
                final String colKey = column.key();
                final long actual = columnValues[i];
                final long expected = column.getLong(i);
                Assert.assertEquals(actual, expected, "Column values match at coordinates (" + rowKey + "," + colKey + ")");
            }
        });
    }


    @Test()
    public void testDoubleStream() throws Exception {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        frame.cols().forEach(column -> {
            final double[] columnValues = column.values().mapToDouble(DataFrameValue::getDouble).toArray();
            for (int i=0; i<column.size(); ++i) {
                final String rowKey = frame.rows().key(i);
                final String colKey = column.key();
                final double actual = columnValues[i];
                final double expected = column.getDouble(i);
                Assert.assertEquals(actual, expected, "Column values match at coordinates (" + rowKey + "," + colKey + ")");
            }
        });
    }


    @Test()
    public void testValueStream() throws Exception {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        frame.cols().forEach(column -> {
            final Object[] columnValues = column.toValueStream().toArray();
            for (int i=0; i<column.size(); ++i) {
                final String rowKey = frame.rows().key(i);
                final String colKey = column.key();
                final Object actual = columnValues[i];
                final Object expected = column.getValue(i);
                Assert.assertEquals(actual, expected, "Column values match at coordinates (" + rowKey + "," + colKey + ")");
            }
        });
    }


    @Test(dataProvider = "args1")
    public void testColumnIterator(Class type) {
        final DataFrame<String, String> frame = TestDataFrames.random(type, 20, 20);
        frame.cols().forEach(column -> {
            int count = 0;
            final Iterator<Object> iterator1 = column.toValueStream().iterator();
            final Iterator<DataFrameValue<String, String>> iterator2 = column.iterator();
            while (iterator1.hasNext()) {
                ++count;
                final Object v1 = iterator1.next();
                final DataFrameValue<String, String> value = iterator2.next();
                final Object v2 = value.getValue();
                Assert.assertEquals(v2, v1, "Value match at " + value.rowKey() + ", " + value.colKey());
            }
            final int rowCount = column.size();
            Assert.assertEquals(rowCount, count, "The row count matches");
        });
    }


    @Test()
    public void testColumnIteratorWithPredicate() throws IOException {
        final DataFrame<String, String> frame = TestDataFrames.random(double.class, 20, 20);
        frame.applyValues(v -> Math.random() * 10);
        frame.rowAt("R5").applyDoubles(v -> Double.NaN);
        frame.rowAt("R10").applyDoubles(v -> Double.NaN);
        frame.cols().forEach(column -> {
            int count = 0;
            final PrimitiveIterator.OfDouble iterator1 = column.values().filter(v -> !v.isNull()).mapToDouble(DataFrameValue::getDouble).iterator();
            final Iterator<DataFrameValue<String, String>> iterator2 = column.iterator(v -> !Double.isNaN(v.getDouble()));
            while (iterator1.hasNext()) {
                ++count;
                final double v1 = iterator1.next();
                Assert.assertTrue(iterator2.hasNext(), "Column iterator has next");
                final DataFrameValue<String, String> value = iterator2.next();
                final double v2 = value.getDouble();
                Assert.assertEquals(v2, v1, "Value match at " + value.rowKey() + ", " + value.colKey());
            }
            final int colCount = column.size() - 2;
            Assert.assertEquals(colCount, count, "The row count matches");
        });
    }


    @Test(dataProvider = "args1")
    public void testColumnToDataFrame(Class type) throws Exception {
        final DataFrame<String, String> frame = TestDataFrames.random(type, 10, 10);
        final DataFrameColumn<String,String> column = frame.colAt("C5");
        if (type == boolean.class) {
            column.forEachValue(v -> Assert.assertEquals(v.getBoolean(), frame.data().getBoolean(v.rowKey(), v.colKey())));
            final DataFrame<String,String> columnFrame = column.toDataFrame();
            column.forEachValue(v -> Assert.assertEquals(v.getBoolean(), columnFrame.data().getBoolean(v.rowKey(), v.colKey())));
        } else if (type == int.class) {
            column.forEachValue(v -> Assert.assertEquals(v.getInt(), frame.data().getInt(v.rowKey(), v.colKey())));
            final DataFrame<String,String> columnFrame = column.toDataFrame();
            column.forEachValue(v -> Assert.assertEquals(v.getInt(), columnFrame.data().getInt(v.rowKey(), v.colKey())));
        } else if (type == long.class) {
            column.forEachValue(v -> Assert.assertEquals(v.getLong(), frame.data().getLong(v.rowKey(), v.colKey())));
            final DataFrame<String,String> columnFrame = column.toDataFrame();
            column.forEachValue(v -> Assert.assertEquals(v.getLong(), columnFrame.data().getLong(v.rowKey(), v.colKey())));
        } else if (type == double.class) {
            column.forEachValue(v -> Assert.assertEquals(v.getDouble(), frame.data().getDouble(v.rowKey(), v.colKey())));
            final DataFrame<String,String> columnFrame = column.toDataFrame();
            column.forEachValue(v -> Assert.assertEquals(v.getDouble(), columnFrame.data().getDouble(v.rowKey(), v.colKey())));
        } else if (type == String.class) {
            column.forEachValue(v -> Assert.assertEquals(v.getValue().toString(), frame.data().getValue(v.rowKey(), v.colKey()).toString()));
            final DataFrame<String,String> columnFrame = column.toDataFrame();
            column.forEachValue(v -> Assert.assertEquals(v.getValue().toString(), columnFrame.data().getValue(v.rowKey(), v.colKey()).toString()));
        } else {
            throw new Exception("Unsupported type: " + type);
        }
    }


    @Test(dataProvider="args1")
    public void testColumnMapping1(Class type) {
        final DataFrame<String,String> frame = TestDataFrames.random(type, 100, 100);
        frame.cols().forEach(column -> {
            if      (type == boolean.class) column.applyBooleans(v -> v.colOrdinal() % 2 == 0);
            else if (type == int.class) column.applyInts(v -> v.colOrdinal() * 10);
            else if (type == long.class) column.applyLongs(v -> v.colOrdinal() * 10);
            else if (type == double.class) column.applyDoubles(v -> v.colOrdinal() * 10d);
            else if (type == String.class) column.applyValues(v -> v.rowOrdinal() + "," + v.colOrdinal());
            else if (type == Object.class) column.applyValues(v -> v.rowOrdinal() + "," + v.colOrdinal());
            else throw new IllegalArgumentException("Unexpected type: " + type);
        });
        for (int colOrdinal = 0; colOrdinal<frame.colCount(); ++colOrdinal) {
            for (int rowIndex=0; rowIndex<frame.rowCount(); ++rowIndex) {
                if (type == boolean.class)  Assert.assertEquals(frame.data().getBoolean(rowIndex, colOrdinal), colOrdinal % 2 == 0, "Match at " + rowIndex + ", " + colOrdinal);
                if (type == int.class)      Assert.assertEquals(frame.data().getInt(rowIndex, colOrdinal), colOrdinal * 10, "Match at " + rowIndex + ", " + colOrdinal);
                if (type == long.class)     Assert.assertEquals(frame.data().getLong(rowIndex, colOrdinal), colOrdinal * 10L, "Match at " + rowIndex + ", " + colOrdinal);
                if (type == double.class)   Assert.assertEquals(frame.data().getDouble(rowIndex, colOrdinal), colOrdinal * 10d, "Match at " + rowIndex + ", " + colOrdinal);
                if (type == String.class)   Assert.assertEquals(frame.data().getValue(rowIndex, colOrdinal), rowIndex + "," + colOrdinal, "Match at " + rowIndex + ", " + colOrdinal);
                if (type == Object.class)   Assert.assertEquals(frame.data().getValue(rowIndex, colOrdinal), rowIndex + "," + colOrdinal, "Match at " + rowIndex + ", " + colOrdinal);
            }
        }
    }


    @Test(dataProvider="args1")
    public void testColumnMapping2(Class type) {
        final Index<String> rowKeys = Index.of("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");
        final DataFrame<String,String> source = TestDataFrames.random(type, rowKeys, rowKeys);
        final DataFrame<String,String> target = TestDataFrames.random(type, rowKeys, rowKeys);
        if (type == boolean.class) {
            target.cols().keys().forEach(colKey -> target.colAt(colKey).applyBooleans(v -> source.data().getBoolean(v.rowOrdinal(), colKey)));
            DataFrameAsserts.assertEqualsByIndex(source, target);
        } else if (type == int.class) {
            target.cols().keys().forEach(colKey -> target.colAt(colKey).applyInts(v -> source.data().getInt(v.rowKey(), colKey)));
            DataFrameAsserts.assertEqualsByIndex(source, target);
        } else if (type == long.class) {
            target.cols().keys().forEach(colKey -> target.colAt(colKey).applyLongs(v -> source.data().getLong(v.rowKey(), colKey)));
            DataFrameAsserts.assertEqualsByIndex(source, target);
        } else if (type == double.class) {
            target.cols().keys().forEach(colKey -> target.colAt(colKey).applyDoubles(v -> source.data().getDouble(v.rowKey(), colKey)));
            DataFrameAsserts.assertEqualsByIndex(source, target);
        } else {
            target.cols().keys().forEach(colKey -> target.colAt(colKey).applyValues(v -> source.data().getValue(v.rowKey(), colKey)));
            DataFrameAsserts.assertEqualsByIndex(source, target);
        }
    }


    @Test()
    public void testFindFirst() {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        final List<String> keyList = new ArrayList<>();
        final String keyToFind = frame.cols().key(85);
        final DataFrameColumn<String,String> columnToFind = frame.colAt(keyToFind);
        final Optional<DataFrameColumn<String,String>> columnMatch = frame.cols().first(column -> {
            keyList.add(column.key());
            return columnToFind.equals(column);
        });
        Assert.assertTrue(columnMatch.isPresent(), "Found a matching column");
        Assert.assertEquals(columnMatch.get().key(), keyToFind, "The column keys match");
        for (int i=0; i<keyList.size(); ++i) {
            final String expected = frame.cols().key(i);
            final String actual = keyList.get(i);
            Assert.assertEquals(actual, expected, "The keys match");
        }
    }


    @Test()
    public void testFindLast() {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        final List<String> keyList = new ArrayList<>();
        final String keyToFind = frame.cols().key(85);
        final DataFrameColumn<String,String> columnToFind = frame.colAt(keyToFind);
        final Optional<DataFrameColumn<String,String>> match = frame.cols().last(column -> {
            keyList.add(column.key());
            if (column.key().equals(keyToFind)) {
                return columnToFind.equals(column);
            }
            return columnToFind.equals(column);
        });
        Assert.assertTrue(match.isPresent(), "Found a matching column");
        Assert.assertEquals(match.get().key(), keyToFind, "The column keys match");
        for (int i=0; i<keyList.size(); ++i) {
            final String expected = frame.cols().key(frame.colCount()-1-i);
            final String actual = keyList.get(i);
            Assert.assertEquals(actual, expected, "The keys match");
        }
    }


    @Test()
    public void testColumnStats() {
        final DataFrame<String, String> frame = TestDataFrames.random(double.class, 10, 10);
        final Stats<Double> stats1 = frame.colAt("C3").stats();
        final Stats<Double> stats2 = frame.colAt("C3").toDataFrame().stats();
        for (StatType stat : StatType.univariate()) {
            final double v1 = stat.apply(stats1);
            final double v2 = stat.apply(stats2);
            Assert.assertEquals(v1, v2, "The stat for " + stat.name() + " matches");
        }
    }


    @Test()
    public void testSelectWithParallelProcessing() {
        final ZonedDateTime start = ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneId.of("GMT"));
        final ZonedDateTime end = start.plusMinutes(1000000);
        final Duration step = Duration.ofMinutes(1);
        final Index<ZonedDateTime> colKeys = Range.of(start, end, step).toIndex(ZonedDateTime.class);
        final DataFrame<String,ZonedDateTime> frame = DataFrame.of("C", colKeys, Double.class).applyDoubles(v -> Math.random() * 100);
        final long expectedCount = colKeys.keys().filter(t -> t.getDayOfWeek() == DayOfWeek.MONDAY).count();
        final DataFrame<String,ZonedDateTime> select1 = frame.cols().parallel().select(col -> col.key().getDayOfWeek() == DayOfWeek.MONDAY);
        final DataFrame<String,ZonedDateTime> select2 = frame.cols().sequential().select(col -> col.key().getDayOfWeek() == DayOfWeek.MONDAY);
        Assert.assertEquals(select1.colCount(), expectedCount, "Selection 1 has expected column count");
        Assert.assertEquals(select2.colCount(), expectedCount, "Selection 1 has expected column count");
        Assert.assertEquals(select1.rowCount(), 1, "Selection 1 has expected row count");
        Assert.assertEquals(select2.rowCount(), 1, "Selection 1 has expected row count");
        DataFrameAsserts.assertEqualsByIndex(select1, select2);
    }


    @Test()
    public void testStreamOfColumns() {
        final int[] colCount = new int[1];
        final LocalDate start = LocalDate.now().minusYears(10);
        final LocalDate end = start.plusDays(5000);
        final Index<LocalDate> rowKeys = Range.of(start, end).toIndex(LocalDate.class);
        final Index<String> colKeys = Index.of("A", "B", "C", "D", "E", "F", "G", "H");
        final DataFrame<LocalDate,String> frame = DataFrame.of(rowKeys, colKeys, Double.class);
        frame.applyDoubles(v -> Math.random() * 100);
        frame.cols().stream().forEach(column -> {
            colCount[0]++;
            final String colKey = column.key();
            Assert.assertEquals(column.size(), frame.rowCount(), "Row count matched");
            final DataFrameColumn<LocalDate,String> expected = frame.colAt(colKey);
            for (int i=0; i<frame.cols().count(); ++i) {
                final double v1 = column.getDouble(i);
                final double v2 = expected.getDouble(i);
                Assert.assertEquals(v1, v2, "The values match for row: " + colKey);
                Assert.assertEquals(column.stats().mean(), expected.stats().mean(), "Mean matches");
            }
        });
        Assert.assertEquals(colCount[0], frame.cols().count(), "Processed expected number of columns");
    }


    @Test()
    public void testIteratorOfColumns() {
        int colCount = 0;
        final LocalDate start = LocalDate.now().minusYears(10);
        final LocalDate end = start.plusDays(5000);
        final Index<LocalDate> rowKeys = Range.of(start, end).toIndex(LocalDate.class);
        final Index<String> colKeys = Index.of("A", "B", "C", "D", "E", "F", "G", "H");
        final DataFrame<LocalDate,String> frame = DataFrame.of(rowKeys, colKeys, Double.class);
        frame.applyDoubles(v -> Math.random() * 100);
        final Iterator<DataFrameColumn<LocalDate,String>> iterator = frame.cols().iterator();
        while (iterator.hasNext()) {
            colCount++;
            final DataFrameColumn<LocalDate,String> column = iterator.next();
            Assert.assertEquals(column.size(), frame.rowCount(), "Row count matched");
            final String colKey = column.key();
            final DataFrameColumn<LocalDate,String> expected = frame.colAt(colKey);
            for (int i=0; i<frame.cols().count(); ++i) {
                final double v1 = column.getDouble(i);
                final double v2 = expected.getDouble(i);
                Assert.assertEquals(v1, v2, "The values match for column: " + colKey);
                Assert.assertEquals(column.stats().mean(), expected.stats().mean(), "Mean matches");
            }
        }
        Assert.assertEquals(colCount, frame.cols().count(), "Processed expected number of columns");
    }


    @Test()
    public void testColToDataFramePerformance() {
        final Range<Integer> rowKeys = Range.of(1, 100000);
        final Range<Integer> colKeys = Range.of(1, 10);
        final DataFrame<Integer,Integer> frame = DataFrame.ofDoubles(rowKeys, colKeys).applyDoubles(v -> Math.random());
        PerfStat.timeInMillis(100, () -> {
            final DataFrameColumn<Integer,Integer> column = frame.colAt(2);
            final DataFrame<Integer,Integer> colFrame = column.toDataFrame();
            Assert.assertEquals(colFrame.rowCount(), frame.rowCount());
            Assert.assertEquals(column.size(), frame.rowCount());
            return frame;
        }).print();
    }


    @Test()
    public void testColumnToArray() {
        final DataFrame<Integer,String> frame = TestDataFrames.createMixedRandomFrame(Integer.class, 1000);
        frame.cols().forEach(column -> {
            final Array<?> array = column.toArray();
            final Class<?> expectedType = column.typeInfo();
            Assert.assertEquals(array.type(), expectedType, "Type is as expected");
            column.forEachValue(v -> {
                final Object expected = v.getValue();
                final Object actual = array.getValue(v.rowOrdinal());
                Assert.assertEquals(actual, expected, "Values for column match for " + v.rowKey());
            });
        });
    }

}
