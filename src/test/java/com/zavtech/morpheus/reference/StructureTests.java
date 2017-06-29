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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameColumn;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameRow;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.range.Range;

/**
 * A test which adds/removes rows and columns and asserts the resulting state is as expected
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class StructureTests {

    private static Index<String> rows = Range.of(0, 200).map(i -> "R" + i).toIndex(String.class);
    private static Index<String> columns = Range.of(0, 100).map(i -> "C" + i).toIndex(String.class);

    /**
     * Constructor
     */
    public StructureTests() {
        super();
    }

    @DataProvider(name = "styles")
    public static Object[][] parallel() {
        return new Object[][] { {true}, {false} };
    }


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
    public void testGeneral(Class type) {
        final DataFrame<String,String> frame = TestDataFrames.random(type, rows, columns);

        Assert.assertEquals(frame.rows().firstKey().get(), "R0", "The first row key");
        Assert.assertEquals(frame.rows().lastKey().get(), "R199", "The last row key");
        Assert.assertTrue(frame.rows().containsAll(Arrays.asList("R0", "R16", "R134", "R159")));
        Assert.assertTrue(!frame.rows().containsAll(Arrays.asList("R0", "R16", "R2340", "R359")));

        Assert.assertEquals(frame.cols().firstKey().get(), "C0", "The first columns key");
        Assert.assertEquals(frame.cols().lastKey().get(), "C99", "The first columns key");
        Assert.assertTrue(frame.cols().containsAll(Arrays.asList("C0", "C25", "C18", "C89")));
        Assert.assertTrue(!frame.cols().containsAll(Arrays.asList("C0", "C25", "C18", "C899")));
    }


    @Test(dataProvider="frameTypes")
    public void testAddingOneRow(Class type) {
        final DataFrame<String,String> expected = TestDataFrames.random(type, rows, columns);
        final DataFrame<String,String> actual = TestDataFrames.random(type, Index.of(rows.getKey(0)), columns);
        expected.rows().keys().forEach(key -> {
            actual.rows().add(key);
            actual.rowAt(key).applyValues(v -> expected.data().getValue(v.rowKey(), v.colKey()));
            DataFrameAsserts.assertEqualsByIndex(expected.select(r -> actual.rows().contains(r.key()), c -> actual.cols().contains(c.key())), actual);
        });
    }

    @Test(dataProvider="frameTypes")
    public void testAddingMultipleRows(Class type) throws Exception {
        final DataFrame<String,String> expected = TestDataFrames.random(type, rows, columns);
        final DataFrame<String,String> actual = TestDataFrames.random(type, Index.of(rows.getKey(0)), columns);
        actual.rows().addAll(expected.rows().keyArray());
        actual.rows().forEach(row -> row.forEachValue(v -> {
            final String rowKey = v.rowKey();
            final String colKey = v.colKey();
            final Object value = expected.data().getValue(rowKey, colKey);
            v.setValue(value);
        }));
        DataFrameAsserts.assertEqualsByIndex(expected, expected);
    }


    @Test(dataProvider="frameTypes")
    public void testAddingOneColumn(Class type) throws Exception {
        final DataFrame<String,String> expected = TestDataFrames.random(type, rows, columns);
        final DataFrame<String,String> actual = TestDataFrames.random(type, rows, Index.of(columns.getKey(0)));
        expected.cols().keys().forEach(key -> {
            actual.cols().add(key, type);
            actual.colAt(key).forEachValue(v -> v.setValue(expected.data().getValue(v.rowKey(), v.colKey())));
            DataFrameAsserts.assertEqualsByIndex(expected.select(row -> actual.rows().contains(row.key()), c -> actual.cols().contains(c.key())), actual);
        });
    }


    @Test(dataProvider="frameTypes")
    public void testAddingMultipleColumns(Class type) throws Exception {
        final DataFrame<String,String> expected = TestDataFrames.random(type, rows, columns);
        final DataFrame<String,String> actual = TestDataFrames.random(type, rows, Index.of(columns.getKey(0)));
        final Array<String> colKeys = expected.cols().keyArray();
        actual.cols().addAll(colKeys, type);
        actual.cols().keys().forEach(key -> {
            actual.colAt(key).forEachValue(v -> {
                v.setValue(expected.data().getValue(v.rowKey(), v.colKey()));
            });
        });
        DataFrameAsserts.assertEqualsByIndex(expected, actual);
    }


    @Test(dataProvider="frameTypes")
    public void testRowSelectionOfOne(Class type) throws Exception {
        final DataFrame<String,String> frame = TestDataFrames.random(type, rows, columns);
        final DataFrameRow<String,String> row = frame.rowAt("R2");
        row.forEachValue(value -> {
            Object actual = value.getValue();
            Object expected = frame.data().getValue(value.rowKey(), value.colKey());
            Assert.assertEquals(actual, expected, "Values match at " + value.rowKey() + ", " + value.colKey());
        });
    }


    @Test(dataProvider="frameTypes")
    public void testColumnSelectionOfOne(Class type) throws Exception {
        final DataFrame<String,String> frame = TestDataFrames.random(type, rows, columns);
        final DataFrameColumn<String,String> column = frame.colAt("C4");
        column.forEachValue(value -> {
            Object actual = value.getValue();
            Object expected = frame.data().getValue(value.rowKey(), value.colKey());
            Assert.assertEquals(actual, expected, "Values match at " + value.rowKey() + ", " + value.colKey());
        });
    }


    @Test(dataProvider="frameTypes", expectedExceptions = {DataFrameException.class})
    public void testDataFrameSelection(Class type) throws Exception {
        final Index<String> rowKeys = Index.of("R1", "R7", "R9", "R14");
        final Index<String> colKeys = Index.of("C4", "C5", "C9", "C22", "C18");
        final DataFrame<String,String> frame = TestDataFrames.random(type, rowKeys, colKeys);
        final DataFrame<String,String> selection = frame.select(r -> rowKeys.contains(r.key()), c -> colKeys.contains(c.key()));
        assertEquals(4, selection.rowCount(), "The slice row count");
        assertEquals(5, selection.colCount(), "The slice column count");
        rowKeys.keys().forEach(row -> assertTrue(selection.rows().contains(row), "The row " + row + " exists"));
        colKeys.keys().forEach(col -> assertTrue(selection.cols().contains(col), "The col " + col + " exists"));
        for (int i=0; i<frame.rowCount(); ++i) {
            for (int j = 0; j<frame.colCount(); ++j) {
                final Object actual = selection.data().getValue(i, j);
                final Object expected = selection.data().getValue(i, j);
                Assert.assertEquals(actual, expected, "Values match at " + i + ", " + j);
            }
        }
        selection.rows().add("C29393");
    }


    @Test()
    public void testFindFirst() {
        final DataFrame<String,String> frame = DataFrame.of(rows, columns, Double.class);
        frame.rows().forEach(row -> row.applyDoubles(v -> Math.random() * 100d));
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
    public void testFindLast() {
        final DataFrame<String,String> frame = DataFrame.of(rows, columns, Double.class);
        frame.rows().forEach(row -> row.applyDoubles(v -> Math.random() * 100d));
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


    @Test(dataProvider = "styles")
    public void testSelectSubset(boolean parallel) {
        final Range<Integer> rowKeys = Range.of(0, 10000);
        final Range<Integer> colKeys = Range.of(0, 20);
        final DataFrame<Integer,Integer> frame = DataFrame.ofDoubles(rowKeys, colKeys).applyDoubles(v -> Math.random());
        final DataFrame<Integer,Integer> source = parallel ? frame.parallel() : frame;
        final DataFrame<Integer,Integer> selection = source.select(row -> row.key() % 2 == 0, col -> col.key() % 2 == 0);

        assertEquals(selection.rowCount(), frame.rowCount() / 2);
        assertEquals(selection.colCount(), frame.colCount() / 2);
        selection.rows().forEach(row -> assertTrue(row.key() % 2 == 0));
        selection.cols().forEach(col -> assertTrue(col.key() % 2 == 0));

        selection.forEachValue(v -> {
            final Integer rowKey = v.rowKey();
            final Integer colKey = v.colKey();
            final double actual = v.getDouble();
            final double expected = source.data().getDouble(rowKey, colKey);
            assertEquals(actual, expected, 0.00001, "Values match at coordinates (" + rowKey + ", " + colKey + ")");
        });
    }


}

