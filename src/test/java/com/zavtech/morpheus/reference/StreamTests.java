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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameValue;
import com.zavtech.morpheus.util.PerfStat;

/**
 * Unit test for various stream interfaces on a DataFrame and its rows and columns
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class StreamTests {


    @Test()
    public void testForEach() throws Exception {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        frame.forEachValue(value -> {
            final String row = value.rowKey();
            final String column = value.colKey();
            final int rowIndex = frame.rows().ordinalOf(row);
            final int colIndex = frame.cols().ordinalOf(column);
            final double iteratorValue = value.getDouble();
            final double expectedValue = frame.data().getDouble(rowIndex, colIndex);
            assertEquals("The row keys match", frame.rows().key(rowIndex), row);
            assertEquals("The column keys match", frame.cols().key(colIndex), column);
            assertEquals("Iterator values match for coordinates (" + rowIndex + "," + colIndex + ")", expectedValue, iteratorValue);
        });
    }


    @Test()
    public void testIntStream() throws Exception {
        final DataFrame<String,String> frame = TestDataFrames.random(int.class, 100, 100);
        final int[] values1 = frame.values().mapToInt(DataFrameValue::getInt).toArray();
        for (int colIndex = 0; colIndex<frame.colCount(); ++colIndex) {
            for (int rowIndex=0; rowIndex<frame.rowCount(); ++rowIndex) {
                final int index = colIndex * frame.rowCount() + rowIndex;
                final int expected = values1[index];
                final int actual= frame.data().getInt(rowIndex, colIndex);
                Assert.assertEquals(actual, expected, "Values match at coordinates (" + rowIndex + "," + colIndex + ")");
            }
        }
    }


    @Test()
    public void testLongStream() throws Exception {
        final DataFrame<String,String> frame = TestDataFrames.random(long.class, 100, 100);
        final long[] values1 = frame.values().mapToLong(DataFrameValue::getLong).toArray();
        for (int colIndex = 0; colIndex<frame.colCount(); ++colIndex) {
            for (int rowIndex=0; rowIndex<frame.rowCount(); ++rowIndex) {
                final int index = colIndex * frame.rowCount() + rowIndex;
                final long expected = values1[index];
                final long actual= frame.data().getLong(rowIndex, colIndex);
                Assert.assertEquals(actual, expected, "Values match at coordinates (" + rowIndex + "," + colIndex + ")");
            }
        }
    }


    @Test()
    public void testDoubleStream() throws Exception {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        final double[] values1 = frame.values().mapToDouble(DataFrameValue::getDouble).toArray();
        for (int colIndex = 0; colIndex<frame.colCount(); ++colIndex) {
            for (int rowIndex=0; rowIndex<frame.rowCount(); ++rowIndex) {
                final int index = colIndex * frame.rowCount() + rowIndex;
                final double expected = values1[index];
                final double actual= frame.data().getDouble(rowIndex, colIndex);
                Assert.assertEquals(actual, expected, "Frame alues match at coordinates (" + rowIndex + "," + colIndex + ")");
            }
        }
    }


    @Test()
    public void testValueStream() throws Exception {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        final Object[] values1 = frame.values().<Object>map(DataFrameValue::getValue).toArray();
        for (int colIndex = 0; colIndex<frame.colCount(); ++colIndex) {
            for (int rowIndex=0; rowIndex<frame.rowCount(); ++rowIndex) {
                final int index = colIndex * frame.rowCount() + rowIndex;
                final Object expected = values1[index];
                final Object actual= frame.data().getDouble(rowIndex, colIndex);
                Assert.assertEquals(actual, expected, "Values match at coordinates (" + rowIndex + "," + colIndex + ")");
            }
        }
    }

    @Test()
    public void testStream() {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 10);

        final double[] sum1 = new double[1];
        final double[] sum2 = new double[1];
        final AtomicInteger counter1 = new AtomicInteger();
        final AtomicInteger counter2 = new AtomicInteger();

        DataFrame<String,String> results = PerfStat.run(1, TimeUnit.MILLISECONDS, false, tasks -> {

            tasks.put("Sequential", () -> {
                frame.values().filter(v -> !v.isNull()).forEach(v -> {
                    try {
                        counter1.incrementAndGet();
                        Thread.sleep(1);
                        synchronized (sum1) {
                            sum1[0]+=v.getDouble();
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                });
                return null;
            });

            tasks.put("Parallel", () -> {
                frame.values().parallel().filter(v -> !v.isNull()).forEach(v -> {
                    try {
                        counter2.incrementAndGet();
                        Thread.sleep(1);
                        synchronized (sum2) {
                            sum2[0]+=v.getDouble();
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                });
                return null;
            });

        });

        results.transpose().out().print();
        Assert.assertEquals(sum1[0], sum2[0], 0.00001, "Sums match");
        Assert.assertEquals(counter1.get(), counter2.get(), "Counts match");
    }

    @Test()
    public void testRowValues() {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 10, 100000);
        frame.rows().forEach(row -> {
            final double[] values1 = row.values().mapToDouble(DataFrameValue::getDouble).toArray();
            final double[] values2 = row.values().mapToDouble(DataFrameValue::getDouble).toArray();
            Assert.assertEquals(values1.length, values2.length, "Array lengths match");
            for (int i=0; i<values1.length; ++i) {
                Assert.assertEquals(values1[i], values1[i], "Values match at index " + i);
            }
        });
    }


    @Test()
    public void testColumnValues() {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100000, 10);
        frame.cols().forEach(column -> {
            final double[] values1 = column.values().mapToDouble(DataFrameValue::getDouble).toArray();
            final double[] values2 = column.values().mapToDouble(DataFrameValue::getDouble).toArray();
            Assert.assertEquals(values1.length, values2.length, "Array lengths match");
            for (int i=0; i<values1.length; ++i) {
                Assert.assertEquals(values1[i], values1[i], "Values match at index " + i);
            }
        });
    }

}
