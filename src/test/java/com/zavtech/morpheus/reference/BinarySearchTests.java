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

import java.util.Optional;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameValue;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.util.AssertException;

/**
 * Unit tests for the binary search functionality on DataFrameVector implementations
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class BinarySearchTests {

    @Test()
    public void testRowSearchFirstAndLast() {
        final Range<Integer> rowKeys = Range.of(0, 1000000);
        final Array<Double> values = rowKeys.map(i -> Math.random() * 100).toArray().sort(true);
        final DataFrame<String,Integer> frame = DataFrame.of(rowKeys, String.class, columns -> columns.add("Column-1", values)).transpose();

        final double first = values.getDouble(0);
        final Optional<DataFrameValue<String,Integer>> firstMatch = frame.rowAt(0).binarySearch(first);
        Assert.assertTrue(firstMatch.isPresent());
        final double firstActual = frame.rowAt(0).getDouble(firstMatch.get().colOrdinal());
        Assert.assertEquals(frame.cols().ordinalOf(firstMatch.get().colKey()), 0, "Index matches");
        Assert.assertEquals(firstActual, first, "Values match");

        final double last = values.getDouble(values.length()-1);
        final Optional<DataFrameValue<String,Integer>> lastMatch = frame.rowAt(0).binarySearch(last);
        Assert.assertTrue(lastMatch.isPresent());
        final double lastActual = frame.rowAt(0).getDouble(lastMatch.get().colOrdinal());
        Assert.assertEquals(frame.cols().ordinalOf(lastMatch.get().colKey()), values.length()-1, "Index matches");
        Assert.assertEquals(lastActual, last, "Values match");
    }


    @Test()
    public void testColumnSearchFirstAndLast() {
        final Range<Integer> rowKeys = Range.of(0, 1000000);
        final Array<Double> values = rowKeys.map(i -> Math.random() * 100).toArray().sort(true);
        final DataFrame<Integer,String> frame = DataFrame.of(rowKeys, String.class, columns -> columns.add("Column-1", values));

        final double first = values.getDouble(0);
        final Optional<DataFrameValue<Integer,String>> firstMatch = frame.colAt(0).binarySearch(first);
        Assert.assertTrue(firstMatch.isPresent());
        final double firstActual = frame.colAt(0).getDouble(firstMatch.get().rowOrdinal());
        Assert.assertTrue(firstMatch.isPresent());
        Assert.assertEquals(frame.rows().ordinalOf(firstMatch.get().rowKey()), 0, "Index matches");
        Assert.assertEquals(firstActual, first, "Values match");

        final double last = values.getDouble(values.length()-1);
        final Optional<DataFrameValue<Integer,String>> lastMatch = frame.colAt(0).binarySearch(last);
        Assert.assertTrue(lastMatch.isPresent());
        final double lastActual = frame.colAt(0).getDouble(lastMatch.get().rowOrdinal());
        Assert.assertTrue(lastMatch.isPresent());
        Assert.assertEquals(frame.rows().ordinalOf(lastMatch.get().rowKey()), values.length()-1, "Index matches");
        Assert.assertEquals(lastActual, last, "Values match");
    }


    @Test()
    public void testRowSearch() {
        final Range<Integer> rowKeys = Range.of(0, 1000000);
        final Array<Double> values = rowKeys.map(i -> Math.random() * 100).toArray().sort(true);
        final DataFrame<String,Integer> frame = DataFrame.of(rowKeys, String.class, columns -> columns.add("Column-1", values)).transpose();
        final Random random = new Random();
        final Array<Integer> indexes = Range.of(0, 500).map(i -> random.nextInt(values.length())).toArray();
        indexes.forEachValue(v -> {
            final int index = v.getInt();
            final int offset = Math.max(0, index - 1295);
            final int length = Math.min(values.length() - offset, offset + 31374);
            final double target = values.getDouble(index);
            final Optional<DataFrameValue<String,Integer>> match1 = frame.rowAt(0).binarySearch(target);
            final Optional<DataFrameValue<String,Integer>> match2 = frame.rowAt(0).binarySearch(target, Double::compare);
            final Optional<DataFrameValue<String,Integer>> match3 = frame.rowAt(0).binarySearch(offset, length, target, Double::compare);
            Assert.assertTrue(match1.isPresent());
            Assert.assertTrue(match2.isPresent());
            Assert.assertTrue(match3.isPresent());
            Assert.assertEquals(match1.get().colOrdinal(), index);
            Assert.assertEquals(match2.get().colOrdinal(), index);
            Assert.assertEquals(match3.get().colOrdinal(), index);
        });
    }


    @Test()
    public void testColumnSearch1() {
        final Range<Integer> rowKeys = Range.of(0, 1000000);
        final Array<Double> values = rowKeys.map(i -> Math.random() * 100).toArray().sort(true);
        final DataFrame<Integer,String> frame = DataFrame.of(rowKeys, String.class, columns -> columns.add("Column-1", values));
        final Random random = new Random();
        final Array<Integer> indexes = Range.of(0, 500).map(i -> random.nextInt(values.length())).toArray();
        indexes.forEachValue(v -> {
            final int index = v.getInt();
            final int offset = Math.max(0, index - 1295);
            final int length = Math.min(values.length() - offset, offset + 31374);
            final double target = values.getDouble(index);
            final Optional<DataFrameValue<Integer,String>> match1 = frame.colAt(0).binarySearch(target);
            final Optional<DataFrameValue<Integer,String>> match2 = frame.colAt(0).binarySearch(target, Double::compare);
            final Optional<DataFrameValue<Integer,String>> match3 = frame.colAt(0).binarySearch(offset, length, target, Double::compare);
            Assert.assertTrue(match1.isPresent());
            Assert.assertTrue(match2.isPresent());
            Assert.assertTrue(match3.isPresent());
            Assert.assertEquals(match1.get().rowKey(), frame.rows().key(index));
            Assert.assertEquals(match2.get().rowKey(), frame.rows().key(index));
            Assert.assertEquals(match3.get().rowKey(), frame.rows().key(index));
        });
    }

    @Test(expectedExceptions = { AssertException.class })
    public void testSearchFailOnIllegalOffset() {
        final Range<Integer> rowKeys = Range.of(0, 1000);
        final Range<String> colKeys = Range.of(0, 5).map(i -> "Column-" + i);
        final DataFrame<Integer,String> frame = DataFrame.ofDoubles(rowKeys, colKeys, v -> Math.random() * 100);
        frame.colAt(0).binarySearch(-1, frame.rowCount(), 123d, Double::compare);
    }

    @Test(expectedExceptions = { AssertException.class })
    public void testSearchFailOnIllegalLength() {
        final Range<Integer> rowKeys = Range.of(0, 1000);
        final Range<String> colKeys = Range.of(0, 5).map(i -> "Column-" + i);
        final DataFrame<Integer,String> frame = DataFrame.ofDoubles(rowKeys, colKeys, v -> Math.random() * 100);
        frame.colAt(0).binarySearch(100, frame.rowCount(), 123d, Double::compare);
    }

    @Test(expectedExceptions = { AssertException.class })
    public void testSearchFailOnNullComparator() {
        final Range<Integer> rowKeys = Range.of(0, 1000);
        final Range<String> colKeys = Range.of(0, 5).map(i -> "Column-" + i);
        final DataFrame<Integer,String> frame = DataFrame.ofDoubles(rowKeys, colKeys, v -> Math.random() * 100);
        frame.colAt(0).binarySearch(100, frame.rowCount(), 123d, null);
    }

}
