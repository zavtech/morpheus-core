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

import java.util.stream.IntStream;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameCursor;
import com.zavtech.morpheus.range.Range;

/**
 * Tests for DataFrame concatenation
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class CombineTests {


    @Test()
    public void testConcatRows() {

        Range<Integer> rows1 = Range.of(0, 10);
        Range<Integer> rows2 = Range.of(5, 20);
        Array<String> columns1 = Array.of("A", "B", "C");
        Array<String> columns2 = Array.of("A", "B", "C", "D", "E");

        DataFrame<Integer,String> frame1 = DataFrame.ofDoubles(rows1, columns1, v -> Math.random());
        DataFrame<Integer,String> frame2 = DataFrame.ofDoubles(rows2, columns2, v -> Math.random());
        DataFrame<Integer,String> frame3 = DataFrame.concatRows(frame1, frame2);

        frame1.out().print(100);
        frame2.out().print(100);
        frame3.out().print(100);

        Assert.assertEquals(frame3.rowCount(), 20);
        Assert.assertEquals(frame3.colCount(), 3);
        Assert.assertTrue(frame3.rows().containsAll(rows1));
        Assert.assertTrue(frame3.rows().containsAll(rows2));
        Assert.assertTrue(!frame3.cols().contains("D"));
        Assert.assertTrue(!frame3.cols().contains("E"));
        Assert.assertTrue(frame3.cols().containsAll(columns1));
        Assert.assertTrue(System.identityHashCode(frame1) != System.identityHashCode(frame3));
        Assert.assertTrue(System.identityHashCode(frame2) != System.identityHashCode(frame3));

        frame1.forEachValue(v -> {
            final double expected = v.getDouble();
            final double actual = frame3.data().getDouble(v.rowOrdinal(), v.colOrdinal());
            Assert.assertEquals(actual, expected);
        });

        for (int i=10; i<frame3.rowCount(); ++i) {
            for (int j=0; j<frame3.colCount(); ++j) {
                final double expected = frame2.data().getDouble(i-5, j);
                final double actual = frame3.data().getDouble(i, j);
                Assert.assertEquals(actual, expected);
            }
        }
    }


    @Test()
    public void testConcatColumns() {

        Range<Integer> rows1 = Range.of(0, 10);
        Range<Integer> rows2 = Range.of(5, 20);
        Array<String> columns1 = Array.of("A", "B", "C");
        Array<String> columns2 = Array.of("A", "B", "C", "D", "E");

        DataFrame<Integer,String> frame1 = DataFrame.ofDoubles(rows1, columns1, v -> Math.random());
        DataFrame<Integer,String> frame2 = DataFrame.ofDoubles(rows2, columns2, v -> Math.random());
        DataFrame<Integer,String> frame3 = DataFrame.concatColumns(frame1, frame2);

        frame1.out().print(100);
        frame2.out().print(100);
        frame3.out().print(100);

        Assert.assertEquals(frame3.rowCount(), 10);
        Assert.assertEquals(frame3.colCount(), 5);
        Assert.assertTrue(frame3.rows().containsAll(rows1));
        Assert.assertTrue(!frame3.rows().containsAll(Range.of(10, 20)));
        Assert.assertTrue(frame3.cols().containsAll(columns2));
        Assert.assertTrue(System.identityHashCode(frame1) != System.identityHashCode(frame3));
        Assert.assertTrue(System.identityHashCode(frame2) != System.identityHashCode(frame3));

        frame1.forEachValue(v -> {
            final double expected = v.getDouble();
            final double actual = frame3.data().getDouble(v.rowOrdinal(), v.colOrdinal());
            Assert.assertEquals(actual, expected);
        });

        DataFrameCursor<Integer,String> cursor = frame3.cursor();
        IntStream.range(0, 5).forEach(rowIndex -> {
            Array.of("D", "E").forEach(colKey -> {
                final double value = cursor.getDouble();
                Assert.assertTrue(cursor.atKeys(rowIndex, colKey).isNull());
            });
        });

        IntStream.range(5, frame3.rowCount()).forEach(rowIndex -> {
            Array.of("D", "E").forEach(colKey -> {
                final double expected = frame2.data().getDouble(rowIndex-5, colKey);
                final double actual = cursor.atKeys(rowIndex, colKey).getDouble();
                Assert.assertEquals(actual, expected);
            });
        });
    }


    @Test()
    public void testCombineFirst() {
        Range<Integer> rows1 = Range.of(0, 10);
        Range<Integer> rows2 = Range.of(5, 20);
        Array<String> columns1 = Array.of("A", "B", "C");
        Array<String> columns2 = Array.of("A", "B", "C", "D", "E");

        DataFrame<Integer,String> frame1 = DataFrame.ofDoubles(rows1, columns1, v -> Math.random());
        DataFrame<Integer,String> frame2 = DataFrame.ofDoubles(rows2, columns2, v -> Math.random());
        DataFrame<Integer,String> frame3 = DataFrame.combineFirst(frame1, frame2);

        frame1.out().print(100);
        frame2.out().print(100);
        frame3.out().print(100);

        Assert.assertEquals(frame3.rowCount(), 20);
        Assert.assertEquals(frame3.colCount(), 5);
        Assert.assertTrue(frame3.rows().containsAll(rows1));
        Assert.assertTrue(frame3.rows().containsAll(rows2));
        Assert.assertTrue(frame3.cols().containsAll(columns1));
        Assert.assertTrue(frame3.cols().containsAll(columns2));
        Assert.assertTrue(System.identityHashCode(frame1) != System.identityHashCode(frame3));
        Assert.assertTrue(System.identityHashCode(frame2) != System.identityHashCode(frame3));

        frame1.forEachValue(v -> {
            final double expected = v.getDouble();
            final double actual = frame3.data().getDouble(v.rowOrdinal(), v.colOrdinal());
            Assert.assertEquals(actual, expected);
        });

        DataFrameCursor<Integer,String> cursor = frame3.cursor();
        IntStream.range(0, 5).forEach(rowIndex -> {
            Array.of("D", "E").forEach(colKey -> {
                Assert.assertTrue(cursor.atKeys(rowIndex, colKey).isNull());
            });
        });

        for (int i=10; i<frame3.rowCount(); ++i) {
            for (int j=0; j<frame3.colCount(); ++j) {
                final double expected = frame2.data().getDouble(i-5, j);
                final double actual = frame3.data().getDouble(i, j);
                Assert.assertEquals(actual, expected);
            }
        }

        IntStream.range(5, frame3.rowCount()).forEach(rowIndex -> {
            Array.of("D", "E").forEach(colKey -> {
                final double expected = frame2.data().getDouble(rowIndex-5, colKey);
                final double actual = cursor.atKeys(rowIndex, colKey).getDouble();
                Assert.assertEquals(actual, expected);
            });
        });
    }

}
