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
import java.util.OptionalDouble;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.zavtech.morpheus.TestSuite;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameColumn;
import com.zavtech.morpheus.frame.DataFrameColumns;
import com.zavtech.morpheus.frame.DataFrameRow;
import com.zavtech.morpheus.frame.DataFrameRows;
import com.zavtech.morpheus.frame.DataFrameValue;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.util.Tuple;

/**
 * Unit tests for the DataFrame min() / max() functions.
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class MinMaxTests {


    @DataProvider(name = "parallel")
    public Object[][] parallel() {
        return new Object[][] { {false}, {true} };
    }


    @Test()
    public void testMin() {
        AtomicInteger counter = new AtomicInteger();
        Range<Integer> keys = Range.of(0, 10);
        DataFrame<Integer,Integer> frame = DataFrame.ofInts(keys, keys, v -> counter.incrementAndGet());

        Optional<DataFrameValue<Integer,Integer>> result0 = frame.min();
        Assert.assertTrue(result0.isPresent());
        Assert.assertEquals(result0.get().getInt(), 1);

        Optional<DataFrameValue<Integer,Integer>> result1 = frame.transpose().min();
        Assert.assertTrue(result1.isPresent());
        Assert.assertEquals(result1.get().getInt(), 1);
    }


    @Test()
    public void testMax() {
        AtomicInteger counter = new AtomicInteger();
        Range<Integer> keys = Range.of(0, 10);
        DataFrame<Integer,Integer> frame = DataFrame.ofInts(keys, keys, v -> counter.incrementAndGet());

        Optional<DataFrameValue<Integer,Integer>> result0 = frame.max();
        Assert.assertTrue(result0.isPresent());
        Assert.assertEquals(result0.get().getInt(), 100);

        Optional<DataFrameValue<Integer,Integer>> result1 = frame.transpose().max();
        Assert.assertTrue(result1.isPresent());
        Assert.assertEquals(result1.get().getInt(), 100);
    }


    @Test()
    public void testMinWithPredicate() {
        AtomicInteger counter = new AtomicInteger();
        Range<Integer> keys = Range.of(0, 10);
        DataFrame<Integer,Integer> frame = DataFrame.ofInts(keys, keys, v -> counter.incrementAndGet());

        Optional<DataFrameValue<Integer,Integer>> result0 = frame.min(v -> v.getInt() > 20);
        Assert.assertTrue(result0.isPresent());
        Assert.assertEquals(result0.get().getInt(), 21);

        Optional<DataFrameValue<Integer,Integer>> result1 = frame.transpose().min(v -> v.getInt() > 20);
        Assert.assertTrue(result1.isPresent());
        Assert.assertEquals(result1.get().getInt(), 21);

    }


    @Test()
    public void testMaxWithPredicate() {
        AtomicInteger counter = new AtomicInteger();
        Range<Integer> keys = Range.of(0, 10);
        DataFrame<Integer,Integer> frame = DataFrame.ofInts(keys, keys, v -> counter.incrementAndGet());

        Optional<DataFrameValue<Integer,Integer>> result0 = frame.max(v -> v.getInt() < 82);
        Assert.assertTrue(result0.isPresent());
        Assert.assertEquals(result0.get().getInt(), 81);

        Optional<DataFrameValue<Integer,Integer>> result1 = frame.transpose().max(v -> v.getInt() < 82);
        Assert.assertTrue(result1.isPresent());
        Assert.assertEquals(result1.get().getInt(), 81);
    }


    @Test(dataProvider = "parallel")
    public void testMinWithPredicate2(boolean parallel) {
        double lowerBound = 25d;
        double[] expected = new double[] {Double.MAX_VALUE};
        Range<Integer> rowKeys = Range.of(0, 1000000);
        Range<String> colKeys = Range.of(0, 5).map(i -> "Column-" + i);
        DataFrame<Integer,String> frame = DataFrame.ofDoubles(rowKeys, colKeys, v -> Math.random() * 100);
        frame.forEachValue(v -> {
            double value = v.getDouble();
            if (value > lowerBound && value < expected[0]) {
                expected[0] = value;
            }
        });
        DataFrame<Integer,String> target = parallel ? frame.parallel() : frame.sequential();
        Optional<DataFrameValue<Integer,String>> result0 = target.min(v -> v.getDouble() > lowerBound);
        Assert.assertTrue(result0.isPresent());
        Assert.assertEquals(result0.get().getValue(), expected[0]);

        //Do same test on the transpose of the test frame
        DataFrame<String,Integer> transpose = parallel ? frame.parallel().transpose() : frame.sequential().transpose();
        Optional<DataFrameValue<String,Integer>> result1 = transpose.min(v -> v.getDouble() > lowerBound);
        Assert.assertTrue(result1.isPresent());
        Assert.assertEquals(result1.get().getValue(), expected[0]);
    }


    @Test(dataProvider = "parallel")
    public void testMaxWithPredicate2(boolean parallel) {
        double upperBound = 25d;
        double[] expected = new double[] {Double.MIN_VALUE};
        Range<Integer> rowKeys = Range.of(0, 1000000);
        Range<String> colKeys = Range.of(0, 5).map(i -> "Column-" + i);
        DataFrame<Integer,String> frame = DataFrame.ofDoubles(rowKeys, colKeys, v -> Math.random() * 100);
        frame.forEachValue(v -> {
            double value = v.getDouble();
            if (value < upperBound && value > expected[0]) {
                expected[0] = value;
            }
        });
        DataFrame<Integer,String> target = parallel ? frame.parallel() : frame.sequential();
        Optional<DataFrameValue<Integer,String>> result = target.max(v -> v.getDouble() < upperBound);
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(result.get().getValue(), expected[0]);

        //Do same test on the transpose of the test frame
        DataFrame<String,Integer> transpose = parallel ? frame.parallel().transpose() : frame.sequential().transpose();
        Optional<DataFrameValue<String,Integer>> result1 = transpose.max(v -> v.getDouble() < upperBound);
        Assert.assertTrue(result1.isPresent());
        Assert.assertEquals(result1.get().getValue(), expected[0]);
    }


    @Test(dataProvider = "parallel")
    public void testArgMinForRow(boolean parallel) {
        final DataFrame<String,String> frame = TestDataFrames.random(Double.class, 10, 100000).applyDoubles(v -> Math.random());
        final Optional<DataFrameValue<String,String>> match = parallel ? frame.rowAt(5).parallel().min(DataFrameValue.doubleComparator()) : frame.rowAt(5).min(DataFrameValue.doubleComparator());
        assertTrue(match.isPresent());
        match.ifPresent(v -> {
            final double minExpected = frame.rowAt(5).stats().min();
            final double minActual = frame.data().getDouble(5, v.colOrdinal());
            assertEquals(minActual, minExpected, 0.00001);
        });
    }


    @Test(dataProvider = "parallel")
    public void testArgMinForColumn(boolean parallel) {
        final DataFrame<String,String> frame = TestDataFrames.random(Double.class, 100000, 10).applyDoubles(v -> Math.random());
        final Optional<DataFrameValue<String,String>> match = parallel ? frame.colAt(5).parallel().min(DataFrameValue.doubleComparator()) : frame.colAt(5).min(DataFrameValue.doubleComparator());
        assertTrue(match.isPresent());
        match.ifPresent(v -> {
            final double minExpected = frame.colAt(5).stats().min();
            final double minActual = frame.data().getDouble(v.rowOrdinal(), 5);
            assertEquals(minActual, minExpected, 0.00001);
        });
    }


    @Test(dataProvider = "parallel")
    public void testArgMaxForRow(boolean parallel) {
        final DataFrame<String,String> frame = TestDataFrames.random(Double.class, 10, 100000).applyDoubles(v -> Math.random());
        final Optional<DataFrameValue<String,String>> match = parallel ? frame.rowAt(5).parallel().max(DataFrameValue.doubleComparator()) : frame.rowAt(5).max(DataFrameValue.doubleComparator());
        assertTrue(match.isPresent());
        match.ifPresent(v -> {
            final double minExpected = frame.rowAt(5).stats().max();
            final double minActual = frame.data().getDouble(5, v.colOrdinal());
            assertEquals(minActual, minExpected, 0.00001);
        });
    }


    @Test(dataProvider = "parallel")
    public void testArgMaxForColumn(boolean parallel) {
        final DataFrame<String,String> frame = TestDataFrames.random(Double.class, 100000, 10).applyDoubles(v -> Math.random());
        final DataFrameColumn<String,String> column = parallel ? frame.colAt(5).parallel() : frame.colAt(5);
        final Optional<DataFrameValue<String,String>> match = column.max(DataFrameValue.doubleComparator());
        assertTrue(match.isPresent());
        match.ifPresent(v -> {
            final double expected = frame.colAt(5).stats().max();
            final double actual = frame.data().getDouble(v.rowOrdinal(), 5);
            assertEquals(actual, expected, 0.00001);
        });
    }


    @Test(dataProvider = "parallel")
    public void testMinRow(boolean parallel) {
        final DataFrame<String,String> frame = TestDataFrames.random(Double.class, 100000, 10).applyDoubles(v -> Math.random() * 10d);
        final DataFrameRows<String,String> rows = parallel ? frame.rows().parallel() : frame.rows();
        final Optional<DataFrameRow<String,String>> rowMatch = rows.min((row1, row2) -> {
            final double diff1 = row1.getDouble("C5") - row1.getDouble("C8");
            final double diff2 = row2.getDouble("C5") - row2.getDouble("C8");
            return Double.compare(Math.abs(diff1), Math.abs(diff2));
        });
        assertTrue(rowMatch.isPresent(), "Row was matched");
        final DataFrameRow<String,String> minRow = rowMatch.get();
        final OptionalDouble expectedMin = frame.rows().stream().mapToDouble(row -> Math.abs(row.getDouble("C5") - row.getDouble("C8"))).min();
        final double actualMin = Math.abs(minRow.getDouble("C5") - minRow.getDouble("C8"));
        assertTrue(expectedMin.isPresent());
        System.out.println("Min diff for " + minRow.key() + " for row " + minRow.ordinal());
        assertEquals(actualMin, expectedMin.getAsDouble(), 0.0001);
    }


    @Test(dataProvider = "parallel")
    public void testMinColumn(boolean parallel) {
        final DataFrame<String,String> frame = TestDataFrames.random(Double.class, 100000, 10).applyDoubles(v -> Math.random() * 10d);
        final DataFrameColumns<String,String> columns = parallel ? frame.cols().parallel() : frame.cols();
        final Optional<DataFrameColumn<String,String>> colMatch = columns.min((col1, col2) -> {
            final double diff1 = col1.getDouble("R5") - col1.getDouble("R8");
            final double diff2 = col2.getDouble("R5") - col2.getDouble("R8");
            return Double.compare(Math.abs(diff1), Math.abs(diff2));
        });
        assertTrue(colMatch.isPresent(), "Column was matched");
        final DataFrameColumn<String,String> column = colMatch.get();
        final OptionalDouble expectedMin = frame.cols().stream().mapToDouble(col -> Math.abs(col.getDouble("R5") - col.getDouble("R8"))).min();
        final double actualMin = Math.abs(column.getDouble("R5") - column.getDouble("R8"));
        assertTrue(expectedMin.isPresent());
        System.out.println("Min diff for " + column.key() + " for row " + column.ordinal());
        assertEquals(actualMin, expectedMin.getAsDouble(), 0.0001);
    }


    @Test(dataProvider = "parallel")
    public void testMaxRow(boolean parallel) {
        final DataFrame<String,String> frame = TestDataFrames.random(Double.class, 100000, 10).applyDoubles(v -> Math.random() * 10d);
        final DataFrameRows<String,String> rows = parallel ? frame.rows().parallel() : frame.rows();
        final Optional<DataFrameRow<String,String>> rowMatch = rows.max((row1, row2) -> {
            final double diff1 = row1.getDouble("C5") - row1.getDouble("C8");
            final double diff2 = row2.getDouble("C5") - row2.getDouble("C8");
            return Double.compare(Math.abs(diff1), Math.abs(diff2));
        });
        assertTrue(rowMatch.isPresent(), "Row was matched");
        final DataFrameRow<String,String> minRow = rowMatch.get();
        final OptionalDouble expectedMin = frame.rows().stream().mapToDouble(row -> Math.abs(row.getDouble("C5") - row.getDouble("C8"))).max();
        final double actualMin = Math.abs(minRow.getDouble("C5") - minRow.getDouble("C8"));
        assertTrue(expectedMin.isPresent());
        System.out.println("Min diff for " + minRow.key() + " for row " + minRow.ordinal());
        assertEquals(actualMin, expectedMin.getAsDouble(), 0.0001);
    }


    @Test(dataProvider = "parallel")
    public void testMaxColumn(boolean parallel) {
        final DataFrame<String,String> frame = TestDataFrames.random(Double.class, 100000, 10).applyDoubles(v -> Math.random() * 10d);
        final DataFrameColumns<String,String> columns = parallel ? frame.cols().parallel() : frame.cols();
        final Optional<DataFrameColumn<String,String>> colMatch = columns.max((col1, col2) -> {
            final double diff1 = col1.getDouble("R5") - col1.getDouble("R8");
            final double diff2 = col2.getDouble("R5") - col2.getDouble("R8");
            return Double.compare(Math.abs(diff1), Math.abs(diff2));
        });
        assertTrue(colMatch.isPresent(), "Column was matched");
        final DataFrameColumn<String,String> column = colMatch.get();
        final OptionalDouble expectedMin = frame.cols().stream().mapToDouble(col -> Math.abs(col.getDouble("R5") - col.getDouble("R8"))).max();
        final double actualMin = Math.abs(column.getDouble("R5") - column.getDouble("R8"));
        assertTrue(expectedMin.isPresent());
        System.out.println("Min diff for " + column.key() + " for row " + column.ordinal());
        assertEquals(actualMin, expectedMin.getAsDouble(), 0.0001);
    }


    @Test()
    public void testPopulation() {

        //Load ONS population dataset
        final DataFrame<Tuple,String> frame = TestSuite.getPopulationDataset();

        //Find row with largest difference between the male/female percentages per year & borough
        final Optional<DataFrameRow<Tuple,String>> rowMatch = frame.rows().max((row1, row2) -> {
            final double row1Total = row1.getDouble("All Persons");
            final double row2Total = row2.getDouble("All Persons");
            final double row1Diff = row1.getDouble("All Males") - row1.getDouble("All Females");
            final double row2Diff = row2.getDouble("All Males") - row2.getDouble("All Females");
            return Double.compare(Math.abs(row1Diff / row1Total), Math.abs(row2Diff / row2Total));
        });

        //Check the data matches expected results
        assertTrue(rowMatch.isPresent());
        rowMatch.ifPresent(maxRow -> {
            final double total = maxRow.getDouble("All Persons");
            final double percentMales = (maxRow.getDouble("All Males") / total) * 100;
            final double percentFemales = (maxRow.getDouble("All Females") / total) * 100d;
            assertEquals(maxRow.key().<Integer>item(0).intValue(), 2011);
            assertEquals(maxRow.key().<String>item(1), "City of London");
            System.out.println("Max Male/Female difference is for " + maxRow.key());
            System.out.println("Male Percent: " + percentMales + "%");
            System.out.println("Female Percent: " + percentFemales + "%");
        });
    }




}
