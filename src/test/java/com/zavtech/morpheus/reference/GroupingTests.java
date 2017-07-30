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

import java.util.Set;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameAsserts;
import com.zavtech.morpheus.frame.DataFrameColumn;
import com.zavtech.morpheus.frame.DataFrameGrouping;
import com.zavtech.morpheus.frame.DataFrameRow;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.util.Tuple;

/**
 * Unit tests for DataFrame grouping functionality
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class GroupingTests {


    @DataProvider(name="parallel")
    public Object[][] parallel() {
        return new Object[][] {{false}, {true}};
    }


    private DataFrame<String,String> frame() {
        return DataFrame.read().csv(options -> {
            options.setResource("/csv/etf.csv");
            options.setHeader(true);
            options.setRowKeyParser(String.class, values -> values[0]);
        });
    }


    @Test(dataProvider = "parallel")
    public void testGroupRows1D(boolean parallel) throws Exception {
        final DataFrame<String,String> source = frame();
        final DataFrameGrouping.Rows<String,String> grouping = parallel ? source.rows().parallel().groupBy("Niche") : source.rows().sequential().groupBy("Niche");
        final Set<String> expectedGroupSet = source.colAt("Niche").<String>toValueStream().distinct().collect(Collectors.toSet());
        Assert.assertEquals(grouping.getDepth(), 1);
        Assert.assertEquals(grouping.getGroupCount(0), expectedGroupSet.size(), "The group count matches");
        grouping.getGroupKeys(0).forEach(groupKey -> {
            final String niche = groupKey.item(0);
            final DataFrame<String,String> group = grouping.getGroup(groupKey);
            final DataFrame<String, String> selection = source.rows().select(row -> niche.equals(row.getValue("Niche")));
            Assert.assertEquals(group.rowCount(), selection.rowCount(), "Row counts match");
            final DataFrameColumn<String, String> aumColumn = selection.colAt("AUM");
            assertEquals(group.colAt("AUM").stats().sum(), aumColumn.stats().sum(), 0.01, "The AUM sums match for " + niche);
            assertEquals(group.colAt("AUM").stats().mean(), aumColumn.stats().mean(), 0.01, "The AUM mean match for " + niche);
            assertEquals(group.colAt("AUM").stats().min(), aumColumn.stats().min(), 0.01, "The AUM min match for " + niche);
            assertEquals(group.colAt("AUM").stats().max(), aumColumn.stats().max(), 0.01, "The AUM max match for " + niche);
            assertEquals(group.colAt("AUM").stats().median(), aumColumn.stats().median(), 0.01, "The AUM median match for " + niche);
            assertEquals(group.colAt("AUM").stats().stdDev(), aumColumn.stats().stdDev(), 0.01, "The AUM stdDevS match for " + niche);
            assertEquals(group.colAt("AUM").stats().kurtosis(), aumColumn.stats().kurtosis(), 0.01, "The AUM kurtosis match for " + niche);
            assertEquals(group.rowCount(), aumColumn.size(), 0.01, "The count matches for " + niche);
        });
    }


    @Test(dataProvider = "parallel")
    public void testGroupRows2D(boolean parallel) throws Exception {
        final DataFrame<String,String> source = frame();
        final DataFrameGrouping.Rows<String,String> grouping = parallel ? source.rows().parallel().groupBy("Issuer", "Niche") : source.rows().groupBy("Issuer", "Niche");
        final Set<Tuple> expectedGroupSet = source.rows().stream().map(row -> Tuple.of(row.<String>getValue("Issuer"))).collect(Collectors.toSet());
        Assert.assertEquals(grouping.getDepth(), 2);
        Assert.assertEquals(grouping.getGroupCount(0), expectedGroupSet.size(), "The group count matches");
        grouping.getGroupKeys(1).forEach(groupKey -> {
            final Object issuer = groupKey.item(0);
            final Object niche = groupKey.item(1);
            final DataFrame<String,String> group = grouping.getGroup(groupKey);
            final DataFrame<String, String> selection = source.rows().select(row -> issuer.equals(row.getValue("Issuer")) && niche.equals(row.getValue("Niche")));
            Assert.assertEquals(group.rowCount(), selection.rowCount(), "Row counts match");
            final DataFrameColumn<String, String> aumColumn = selection.colAt("AUM");
            assertEquals(group.colAt("AUM").stats().sum(), aumColumn.stats().sum(), 0.01, "The AUM sums match for " + niche);
            assertEquals(group.colAt("AUM").stats().mean(), aumColumn.stats().mean(), 0.01, "The AUM mean match for " + niche);
            assertEquals(group.colAt("AUM").stats().min(), aumColumn.stats().min(), 0.01, "The AUM min match for " + niche);
            assertEquals(group.colAt("AUM").stats().max(), aumColumn.stats().max(), 0.01, "The AUM max match for " + niche);
            assertEquals(group.colAt("AUM").stats().median(), aumColumn.stats().median(), 0.01, "The AUM median match for " + niche);
            assertEquals(group.colAt("AUM").stats().stdDev(), aumColumn.stats().stdDev(), 0.01, "The AUM stdDevS match for " + niche);
            assertEquals(group.colAt("AUM").stats().kurtosis(), aumColumn.stats().kurtosis(), 0.01, "The AUM kurtosis match for " + niche);
            assertEquals(group.rowCount(), aumColumn.size(), 0.01, "The count matches for " + niche);
        });
    }


    @Test(dataProvider = "parallel")
    public void testGroupColumns1D(boolean parallel) throws Exception {
        final DataFrame<String,String> source = frame();
        final DataFrame<String,String> frame = source.transpose();
        DataFrameAsserts.assertEqualsByIndex(source, transpose(frame));
        final DataFrameGrouping.Cols<String,String> grouping = parallel ? frame.cols().parallel().groupBy("Niche") : frame.cols().groupBy("Niche");
        final Set<String> expectedGroupSet = frame.rowAt("Niche").<String>toValueStream().distinct().collect(Collectors.toSet());
        Assert.assertEquals(grouping.getDepth(), 1);
        Assert.assertEquals(grouping.getGroupCount(0), expectedGroupSet.size(), "The group count matches");
        grouping.getGroupKeys(0).forEach(groupKey -> {
            final Object niche = groupKey.item(0);
            final DataFrame<String,String> group = grouping.getGroup(groupKey);
            final DataFrame<String,String> selection = frame.cols().select(column -> niche.equals(column.getValue("Niche")));
            Assert.assertEquals(group.colCount(), selection.colCount(), "Column counts match");
            final DataFrameRow<String, String> aumRow = selection.rowAt("AUM");
            assertEquals(group.rowAt("AUM").stats().sum(), aumRow.stats().sum(), 0.01, "The AUM sums match for " + niche);
            assertEquals(group.rowAt("AUM").stats().mean(), aumRow.stats().mean(), 0.01, "The AUM mean match for " + niche);
            assertEquals(group.rowAt("AUM").stats().min(), aumRow.stats().min(), 0.01, "The AUM min match for " + niche);
            assertEquals(group.rowAt("AUM").stats().max(), aumRow.stats().max(), 0.01, "The AUM max match for " + niche);
            assertEquals(group.rowAt("AUM").stats().median(), aumRow.stats().median(), 0.01, "The AUM median match for " + niche);
            assertEquals(group.rowAt("AUM").stats().stdDev(), aumRow.stats().stdDev(), 0.01, "The AUM stdDevS match for " + niche);
            assertEquals(group.rowAt("AUM").stats().kurtosis(), aumRow.stats().kurtosis(), 0.01, "The AUM kurtosis match for " + niche);
            assertEquals(group.colCount(), aumRow.size(), 0.01, "The count matches for " + niche);
        });
    }


    @Test(dataProvider = "parallel")
    public void testGroupColumns2D(boolean parallel) throws Exception {
        final DataFrame<String,String> source = frame();
        final DataFrame<String,String> frame = transpose(source);
        DataFrameAsserts.assertEqualsByIndex(source, transpose(frame));
        final DataFrameGrouping.Cols<String,String> grouping = parallel ? frame.cols().parallel().groupBy("Issuer", "Niche") : frame.cols().groupBy("Issuer", "Niche");
        final Array<Tuple> expectedGroupSet = frame.rowAt("Issuer").distinct().map(v -> Tuple.of(v.<String>getValue()));
        Assert.assertEquals(grouping.getDepth(), 2);
        Assert.assertEquals(grouping.getGroupCount(0), expectedGroupSet.length(), "The group count matches");
        grouping.getGroupKeys(1).forEach(groupKey -> {
            final Object issuer = groupKey.item(0);
            final Object niche = groupKey.item(1);
            final DataFrame<String,String> group = grouping.getGroup(groupKey);
            final DataFrame<String, String> selection = frame.cols().select(column -> issuer.equals(column.getValue("Issuer")) && niche.equals(column.getValue("Niche")));
            Assert.assertEquals(group.colCount(), selection.colCount(), "Column counts match");
            final DataFrameRow<String, String> aumRow = selection.rowAt("AUM");
            assertEquals(group.rowAt("AUM").stats().sum(), aumRow.stats().sum(), 0.01, "The AUM sums match for " + niche);
            assertEquals(group.rowAt("AUM").stats().mean(), aumRow.stats().mean(), 0.01, "The AUM mean match for " + niche);
            assertEquals(group.rowAt("AUM").stats().min(), aumRow.stats().min(), 0.01, "The AUM min match for " + niche);
            assertEquals(group.rowAt("AUM").stats().max(), aumRow.stats().max(), 0.01, "The AUM max match for " + niche);
            assertEquals(group.rowAt("AUM").stats().median(), aumRow.stats().median(), 0.01, "The AUM median match for " + niche);
            assertEquals(group.rowAt("AUM").stats().stdDev(), aumRow.stats().stdDev(), 0.01, "The AUM stdDev match for " + niche);
            assertEquals(group.rowAt("AUM").stats().kurtosis(), aumRow.stats().kurtosis(), 0.01, "The AUM kurtosis match for " + niche);
            assertEquals(group.colCount(), aumRow.size(), 0.01, "The count matches for " + niche);
        });
    }


    @Test()
    public void testTranspose() {
        final DataFrame<String,String> source = frame();
        final DataFrame<String,String> transpose1 = transpose(source);
        final DataFrame<String,String> transpose2 = source.transpose();
        DataFrameAsserts.assertEqualsByIndex(transpose1, transpose2);
    }


    /**
     * Returns a transpose for the input frame
     * @param frame     the frame reference
     * @return          the transpose of the frame
     */
    private DataFrame<String,String> transpose(DataFrame<String,String> frame) {
        final Index<String> rows = Index.of(frame.cols().keyArray());
        final Index<String> columns = Index.of(frame.rows().keyArray());
        final DataFrame<String,String> transpose = DataFrame.ofObjects(rows, columns);
        for (int i=0; i<frame.rowCount(); ++i) {
            for (int j = 0; j<frame.colCount(); ++j) {
                final Object value = frame.data().getValue(i, j);
                transpose.data().setValue(j, i, value);
            }
        }
        return transpose;
    }

    /**
     * Asset between doubles that allows both to be NaN
     * @param value1        the first value
     * @param value2        the second value
     * @param tolerance     the tolerance factor
     * @param msg           the message for assertion error
     */
    static void assertEquals(double value1, double value2, double tolerance, String msg) {
        if (!Double.isNaN(value1) || !Double.isNaN(value2)) {
            final double diff = Math.abs(value2 - value1);
            if (diff > tolerance) {
                throw new AssertionError(msg + ": " + value1 + " != " + value2);
            }
        }
    }
}
