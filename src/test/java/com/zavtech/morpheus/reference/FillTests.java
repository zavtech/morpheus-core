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

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.range.Range;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * A unit test to assess the matrix notification functionality
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class FillTests {



    @Test()
    public void testFillDown() throws IOException {
        final Range<String> rowKeys = Range.of(0, 100, 1).map(i -> "R" + i);
        final Range<String> colKeys = Range.of(0, 10).map(i -> "C" + i);
        final DataFrame<String,String> frame = DataFrame.ofDoubles(rowKeys, colKeys);
        frame.row("R6").applyDoubles(v -> 6d);
        frame.row("R20").applyDoubles(v -> 20d);
        frame.row("R60").applyDoubles(v -> 60d);
        frame.row("R70").applyDoubles(v -> 70d);
        frame.fill().down(100);
        frame.rows().forEach(row -> {
            final int rowIndex = row.ordinal();
            final String rowKey = row.key();
            if (rowIndex < 6) {
                row.forEachValue(v -> assertEquals("Value at row " + rowKey, Double.NaN, v.getDouble()));
            } else if (rowIndex < 20) {
                row.forEachValue(v -> assertEquals("Value at row " + rowKey, 6d, v.getDouble()));
            } else if (rowIndex < 60) {
                row.forEachValue(v -> assertEquals("Value at row " + rowKey, 20d, v.getDouble()));
            } else if (rowIndex < 70) {
                row.forEachValue(v -> assertEquals("Value at row " + rowKey, 60d, v.getDouble()));
            } else if (rowIndex < 100) {
                row.forEachValue(v -> assertEquals("Value at row " + rowKey, 70d, v.getDouble()));
            }
        });
    }


    @Test()
    public void testFillUp() throws IOException {
        final Range<String> rowKeys = Range.of(0, 100, 1).map(i -> "R" + i);
        final Range<String> colKeys = Range.of(0, 10).map(i -> "C" + i);
        final DataFrame<String,String> frame = DataFrame.ofDoubles(rowKeys, colKeys);
        frame.row("R6").applyDoubles(v -> 6d);
        frame.row("R20").applyDoubles(v -> 20d);
        frame.row("R60").applyDoubles(v -> 60d);
        frame.row("R70").applyDoubles(v -> 70d);
        frame.fill().up(100);
        frame.rows().forEach(row -> {
            final int rowIndex = row.ordinal();
            final String rowKey = row.key();
            if (rowIndex <= 6) {
                row.forEachValue(v -> assertEquals("Value at row " + rowKey, 6d, v.getDouble()));
            } else if (rowIndex <= 20) {
                row.forEachValue(v -> assertEquals("Value at row " + rowKey, 20d, v.getDouble()));
            } else if (rowIndex <= 60) {
                row.forEachValue(v -> assertEquals("Value at row " + rowKey, 60d, v.getDouble()));
            } else if (rowIndex <= 70) {
                row.forEachValue(v -> assertEquals("Value at row " + rowKey, 70d, v.getDouble()));
            } else if (rowIndex <= 100) {
                row.forEachValue(v -> assertEquals("Value at row " + rowKey, Double.NaN, v.getDouble()));
            }
        });
    }


    @Test()
    public void testFillRight() throws IOException {
        final Range<String> rowKeys = Range.of(0, 100, 1).map(i -> "R" + i);
        final Range<String> colKeys = Range.of(0, 40).map(i -> "C" + i);
        final DataFrame<String,String> frame = DataFrame.ofDoubles(rowKeys, colKeys);
        frame.col("C6").applyDoubles(v -> 6d);
        frame.col("C12").applyDoubles(v -> 20d);
        frame.col("C20").applyDoubles(v -> 60d);
        frame.col("C30").applyDoubles(v -> 70d);
        frame.fill().right(20);
        frame.out().print();
        frame.cols().forEach(column -> {
            final int colIndex = column.ordinal();
            final String colKey = column.key();
            if (colIndex < 6) {
                column.forEachValue(v -> assertEquals("Value at column " + colKey, Double.NaN, v.getDouble()));
            } else if (colIndex < 12) {
                column.forEachValue(v -> assertEquals("Value at column " + colKey, 6d, v.getDouble()));
            } else if (colIndex < 20) {
                column.forEachValue(v -> assertEquals("Value at column " + colKey, 20d, v.getDouble()));
            } else if (colIndex < 30) {
                column.forEachValue(v -> assertEquals("Value at column " + colKey, 60d, v.getDouble()));
            } else if (colIndex < 40) {
                column.forEachValue(v -> assertEquals("Value at column " + colKey, 70d, v.getDouble()));
            }
        });
    }


    @Test()
    public void testFillLeft() throws IOException {
        final Range<String> rowKeys = Range.of(0, 100, 1).map(i -> "R" + i);
        final Range<String> colKeys = Range.of(0, 40).map(i -> "C" + i);
        final DataFrame<String,String> frame = DataFrame.ofDoubles(rowKeys, colKeys);
        frame.col("C6").applyDoubles(v -> 6d);
        frame.col("C12").applyDoubles(v -> 20d);
        frame.col("C20").applyDoubles(v -> 60d);
        frame.col("C30").applyDoubles(v -> 70d);
        frame.fill().left(20);
        frame.cols().forEach(column -> {
            final int colIndex = column.ordinal();
            final String colKey = column.key();
            if (colIndex <= 6) {
                column.forEachValue(v -> assertEquals("Value at column " + colKey, 6d, v.getDouble()));
            } else if (colIndex <= 12) {
                column.forEachValue(v -> assertEquals("Value at column " + colKey, 20d, v.getDouble()));
            } else if (colIndex <= 20) {
                column.forEachValue(v -> assertEquals("Value at column " + colKey, 60d, v.getDouble()));
            } else if (colIndex <= 30) {
                column.forEachValue(v -> assertEquals("Value at column " + colKey, 70d, v.getDouble()));
            } else if (colIndex < 40) {
                column.forEachValue(v -> assertEquals("Value at column " + colKey, Double.NaN, v.getDouble()));
            }
        });
    }


}
