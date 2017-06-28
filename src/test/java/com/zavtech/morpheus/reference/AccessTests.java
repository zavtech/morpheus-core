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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.range.Range;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests the various methods on the DataFrameAccess interface
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class AccessTests {


    @DataProvider(name="SparseOrDense")
    public Object[][] getSparseOrDense() {
        return new Object[][] { {false}, {true} };
    }


    @DataProvider(name="exceptions")
    public Object[][] getExceptions() {
        return new Object[][] {
            { 0 }, { 1 }, { 2 }, { 3 }, { 4 }, { 5 }, { 6 }, { 7 },
        };
    }

    @DataProvider(name="args2")
    public Object[][] getArgs2() {
        return new Object[][] {
            { 0 }, { 1 }, { 2 },
        };
    }

    @DataProvider(name="args3")
    public Object[][] getArgs3() {
        return new Object[][] {
            { 0 }, { 1 }
        };
    }

    @DataProvider(name="args4")
    public Object[][] getArgs4() {
        return new Object[][] {
            { boolean.class },
            { int.class },
            { long.class },
            { double.class },
            { Object.class },
        };
    }




    @Test()
    public void testBooleanReads() {
        final Random random = new Random(2344);
        final Map<Coordinate,Boolean> valueMap = new HashMap<>();
        final DataFrame<String,String> frame = TestDataFrames.random(boolean.class, 100, 100);
        frame.applyBooleans(v -> random.nextBoolean());
        frame.forEachValue(v -> valueMap.put(new Coordinate(v.rowOrdinal(), v.colOrdinal()), v.getBoolean()));
        for (int i=0; i<frame.rowCount(); ++i) {
            final String rowKey = frame.rows().key(i);
            for (int j = 0; j<frame.colCount(); ++j) {
                final String colKey = frame.cols().key(j);
                final boolean v1 = frame.data().getBoolean(i, j);
                final boolean v2 = frame.data().getBoolean(rowKey, j);
                final boolean v3 = frame.data().getBoolean(i, colKey);
                final boolean v4 = frame.data().getBoolean(rowKey, colKey);
                final Coordinate coordinate = new Coordinate(i,j);
                Assert.assertEquals(v1, v2, "V1 matches v2");
                Assert.assertEquals(v1, v3, "V1 matches v3");
                Assert.assertEquals(v1, v4, "V1 matches v4");
                Assert.assertEquals(v1, valueMap.get(coordinate).booleanValue(), "V1 matches apply value");
            }
        }
    }

    @Test()
    public void testIntReads() {
        final Random random = new Random(2344);
        final Map<Coordinate,Integer> valueMap = new HashMap<>();
        final DataFrame<String,String> frame = TestDataFrames.random(int.class, 100, 100);
        frame.applyInts(v -> random.nextInt() * 10);
        frame.forEachValue(v -> valueMap.put(new Coordinate(v.rowOrdinal(), v.colOrdinal()), v.getInt()));
        for (int i=0; i<frame.rowCount(); ++i) {
            final String rowKey = frame.rows().key(i);
            for (int j = 0; j<frame.colCount(); ++j) {
                final String colKey = frame.cols().key(j);
                final int v1 = frame.data().getInt(i, j);
                final int v2 = frame.data().getInt(rowKey, j);
                final int v3 = frame.data().getInt(i, colKey);
                final int v4 = frame.data().getInt(rowKey, colKey);
                final Coordinate coordinate = new Coordinate(i,j);
                Assert.assertEquals(v1, v2, "V1 matches v2");
                Assert.assertEquals(v1, v3, "V1 matches v3");
                Assert.assertEquals(v1, v4, "V1 matches v4");
                Assert.assertEquals(v1, valueMap.get(coordinate).intValue(), "V1 matches apply value");
            }
        }
    }


    @Test()
    public void testLongReads() {
        final Random random = new Random(2344);
        final Map<Coordinate,Long> valueMap = new HashMap<>();
        final DataFrame<String,String> frame = TestDataFrames.random(long.class, 100, 100);
        frame.applyLongs(v -> random.nextLong() * 10);
        frame.forEachValue(v -> valueMap.put(new Coordinate(v.rowOrdinal(), v.colOrdinal()), v.getLong()));
        for (int i=0; i<frame.rowCount(); ++i) {
            final String rowKey = frame.rows().key(i);
            for (int j = 0; j<frame.colCount(); ++j) {
                final String colKey = frame.cols().key(j);
                final long v1 = frame.data().getLong(i, j);
                final long v2 = frame.data().getLong(rowKey, j);
                final long v3 = frame.data().getLong(i, colKey);
                final long v4 = frame.data().getLong(rowKey, colKey);
                final Coordinate coordinate = new Coordinate(i,j);
                Assert.assertEquals(v1, v2, "V1 matches v2");
                Assert.assertEquals(v1, v3, "V1 matches v3");
                Assert.assertEquals(v1, v4, "V1 matches v4");
                Assert.assertEquals(v1, valueMap.get(coordinate).longValue(), "V1 matches apply value");
            }
        }
    }


    @Test()
    public void testDoubleReads() {
        final Random random = new Random(2344);
        final Map<Coordinate,Double> valueMap = new HashMap<>();
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        frame.applyDoubles(v -> random.nextDouble() * 10);
        frame.forEachValue(v -> valueMap.put(new Coordinate(v.rowOrdinal(), v.colOrdinal()), v.getDouble()));
        for (int i=0; i<frame.rowCount(); ++i) {
            final String rowKey = frame.rows().key(i);
            for (int j = 0; j<frame.colCount(); ++j) {
                final String colKey = frame.cols().key(j);
                final double v1 = frame.data().getDouble(i, j);
                final double v2 = frame.data().getDouble(rowKey, j);
                final double v3 = frame.data().getDouble(i, colKey);
                final double v4 = frame.data().getDouble(rowKey, colKey);
                final Coordinate coordinate = new Coordinate(i,j);
                Assert.assertEquals(v1, v2, "V1 matches v2");
                Assert.assertEquals(v1, v3, "V1 matches v3");
                Assert.assertEquals(v1, v4, "V1 matches v4");
                Assert.assertEquals(v1, valueMap.get(coordinate), "V1 matches apply value");
            }
        }
    }


    @Test()
    public void testValueReads() {
        final Random random = new Random(2344);
        final Map<Coordinate,String> valueMap = new HashMap<>();
        final DataFrame<String,String> frame = TestDataFrames.random(Object.class, 100, 100);
        frame.applyValues(v -> "x:" + (random.nextDouble() * 10));
        frame.forEachValue(v -> valueMap.put(new Coordinate(v.rowOrdinal(), v.colOrdinal()), v.getValue()));
        for (int i=0; i<frame.rowCount(); ++i) {
            final String rowKey = frame.rows().key(i);
            for (int j = 0; j<frame.colCount(); ++j) {
                final String colKey = frame.cols().key(j);
                final String v1 = frame.data().getValue(i, j);
                final String v2 = frame.data().getValue(rowKey, j);
                final String v3 = frame.data().getValue(i, colKey);
                final String v4 = frame.data().getValue(rowKey, colKey);
                final Coordinate coordinate = new Coordinate(i,j);
                Assert.assertEquals(v1, v2, "V1 matches v2");
                Assert.assertEquals(v1, v3, "V1 matches v3");
                Assert.assertEquals(v1, v4, "V1 matches v4");
                Assert.assertEquals(v1, valueMap.get(coordinate), "V1 matches apply value");
            }
        }
    }


    @Test()
    public void testBooleanWrites() {
        final Random random = new Random(2344);
        final Range<String> rows = Range.of(0, 100).map(i -> "R" + i);
        final Range<String> columns = Range.of(0, 100).map(i -> "C" + i);
        final DataFrame<String,String> source = DataFrame.ofBooleans(rows, columns);
        final DataFrame<String,String> frame1 = DataFrame.ofBooleans(rows, columns);
        final DataFrame<String,String> frame2 = DataFrame.ofBooleans(rows, columns);
        final DataFrame<String,String> frame3 = DataFrame.ofBooleans(rows, columns);
        final DataFrame<String,String> frame4 = DataFrame.ofBooleans(rows, columns);
        source.applyBooleans(v -> random.nextBoolean());
        source.forEachValue(v -> {
            frame1.data().setBoolean(v.rowOrdinal(), v.colOrdinal(), source.data().getBoolean(v.rowOrdinal(), v.colOrdinal()));
            frame2.data().setBoolean(v.rowKey(), v.colOrdinal(), source.data().getBoolean(v.rowOrdinal(), v.colOrdinal()));
            frame3.data().setBoolean(v.rowOrdinal(), v.colKey(), source.data().getBoolean(v.rowOrdinal(), v.colOrdinal()));
            frame4.data().setBoolean(v.rowKey(), v.colKey(), source.data().getBoolean(v.rowOrdinal(), v.colOrdinal()));
        });
        DataFrameAsserts.assertEqualsByIndex(source, frame1);
        DataFrameAsserts.assertEqualsByIndex(source, frame2);
        DataFrameAsserts.assertEqualsByIndex(source, frame3);
        DataFrameAsserts.assertEqualsByIndex(source, frame4);
    }


    @Test()
    public void testIntWrites() {
        final Random random = new Random(2344);
        final Range<String> rows = Range.of(0, 100).map(i -> "R" + i);
        final Range<String> columns = Range.of(0, 100).map(i -> "C" + i);
        final DataFrame<String,String> source = DataFrame.ofInts(rows, columns);
        final DataFrame<String,String> frame1 = DataFrame.ofInts(rows, columns);
        final DataFrame<String,String> frame2 = DataFrame.ofInts(rows, columns);
        final DataFrame<String,String> frame3 = DataFrame.ofInts(rows, columns);
        final DataFrame<String,String> frame4 = DataFrame.ofInts(rows, columns);
        source.applyInts(v -> random.nextInt() * 10);
        source.forEachValue(v -> {
            frame1.data().setInt(v.rowOrdinal(), v.colOrdinal(), source.data().getInt(v.rowOrdinal(), v.colOrdinal()));
            frame2.data().setInt(v.rowKey(), v.colOrdinal(), source.data().getInt(v.rowOrdinal(), v.colOrdinal()));
            frame3.data().setInt(v.rowOrdinal(), v.colKey(), source.data().getInt(v.rowOrdinal(), v.colOrdinal()));
            frame4.data().setInt(v.rowKey(), v.colKey(), source.data().getInt(v.rowOrdinal(), v.colOrdinal()));
        });
        DataFrameAsserts.assertEqualsByIndex(source, frame1);
        DataFrameAsserts.assertEqualsByIndex(source, frame2);
        DataFrameAsserts.assertEqualsByIndex(source, frame3);
        DataFrameAsserts.assertEqualsByIndex(source, frame4);
    }


    @Test()
    public void testLongWrites() {
        final Random random = new Random(2344);
        final Range<String> rows = Range.of(0, 100).map(i -> "R" + i);
        final Range<String> columns = Range.of(0, 100).map(i -> "C" + i);
        final DataFrame<String,String> source = DataFrame.ofLongs(rows, columns);
        final DataFrame<String,String> frame1 = DataFrame.ofLongs(rows, columns);
        final DataFrame<String,String> frame2 = DataFrame.ofLongs(rows, columns);
        final DataFrame<String,String> frame3 = DataFrame.ofLongs(rows, columns);
        final DataFrame<String,String> frame4 = DataFrame.ofLongs(rows, columns);
        source.applyLongs(v -> random.nextLong() * 10);
        source.forEachValue(v -> {
            frame1.data().setLong(v.rowOrdinal(), v.colOrdinal(), source.data().getLong(v.rowOrdinal(), v.colOrdinal()));
            frame2.data().setLong(v.rowKey(), v.colOrdinal(), source.data().getLong(v.rowOrdinal(), v.colOrdinal()));
            frame3.data().setLong(v.rowOrdinal(), v.colKey(), source.data().getLong(v.rowOrdinal(), v.colOrdinal()));
            frame4.data().setLong(v.rowKey(), v.colKey(), source.data().getLong(v.rowOrdinal(), v.colOrdinal()));
        });
        DataFrameAsserts.assertEqualsByIndex(source, frame1);
        DataFrameAsserts.assertEqualsByIndex(source, frame2);
        DataFrameAsserts.assertEqualsByIndex(source, frame3);
        DataFrameAsserts.assertEqualsByIndex(source, frame4);
    }


    @Test()
    public void testDoubleWrites() {
        final Random random = new Random(2344);
        final Range<String> rows = Range.of(0, 100).map(i -> "R" + i);
        final Range<String> columns = Range.of(0, 100).map(i -> "C" + i);
        final DataFrame<String,String> source = DataFrame.ofDoubles(rows, columns);
        final DataFrame<String,String> frame1 = DataFrame.ofDoubles(rows, columns);
        final DataFrame<String,String> frame2 = DataFrame.ofDoubles(rows, columns);
        final DataFrame<String,String> frame3 = DataFrame.ofDoubles(rows, columns);
        final DataFrame<String,String> frame4 = DataFrame.ofDoubles(rows, columns);
        source.applyDoubles(v -> random.nextDouble() * 10d);
        source.forEachValue(v -> {
            frame1.data().setDouble(v.rowOrdinal(), v.colOrdinal(), source.data().getDouble(v.rowOrdinal(), v.colOrdinal()));
            frame2.data().setDouble(v.rowKey(), v.colOrdinal(), source.data().getDouble(v.rowOrdinal(), v.colOrdinal()));
            frame3.data().setDouble(v.rowOrdinal(), v.colKey(), source.data().getDouble(v.rowOrdinal(), v.colOrdinal()));
            frame4.data().setDouble(v.rowKey(), v.colKey(), source.data().getDouble(v.rowOrdinal(), v.colOrdinal()));
        });
        DataFrameAsserts.assertEqualsByIndex(source, frame1);
        DataFrameAsserts.assertEqualsByIndex(source, frame2);
        DataFrameAsserts.assertEqualsByIndex(source, frame3);
        DataFrameAsserts.assertEqualsByIndex(source, frame4);
    }


    @Test()
    public void testStringWrites() {
        final Random random = new Random(2344);
        final Range<String> rows = Range.of(0, 100).map(i -> "R" + i);
        final Range<String> columns = Range.of(0, 100).map(i -> "C" + i);
        final DataFrame<String,String> source = DataFrame.ofStrings(rows, columns);
        final DataFrame<String,String> frame1 = DataFrame.ofStrings(rows, columns);
        final DataFrame<String,String> frame2 = DataFrame.ofStrings(rows, columns);
        final DataFrame<String,String> frame3 = DataFrame.ofStrings(rows, columns);
        final DataFrame<String,String> frame4 = DataFrame.ofStrings(rows, columns);
        source.applyValues(v -> "X: " + random.nextDouble() * 10d);
        source.forEachValue(v -> {
            frame1.data().setValue(v.rowOrdinal(), v.colOrdinal(), source.data().getValue(v.rowOrdinal(), v.colOrdinal()));
            frame2.data().setValue(v.rowKey(), v.colOrdinal(), source.data().getValue(v.rowOrdinal(), v.colOrdinal()));
            frame3.data().setValue(v.rowOrdinal(), v.colKey(), source.data().getValue(v.rowOrdinal(), v.colOrdinal()));
            frame4.data().setValue(v.rowKey(), v.colKey(), source.data().getValue(v.rowOrdinal(), v.colOrdinal()));
        });
        DataFrameAsserts.assertEqualsByIndex(source, frame1);
        DataFrameAsserts.assertEqualsByIndex(source, frame2);
        DataFrameAsserts.assertEqualsByIndex(source, frame3);
        DataFrameAsserts.assertEqualsByIndex(source, frame4);
    }


    @Test()
    public void testObjectWrites() {
        final Random random = new Random(2344);
        final Range<String> rows = Range.of(0, 100).map(i -> "R" + i);
        final Range<String> columns = Range.of(0, 100).map(i -> "C" + i);
        final DataFrame<String,String> source = DataFrame.ofObjects(rows, columns);
        final DataFrame<String,String> frame1 = DataFrame.ofObjects(rows, columns);
        final DataFrame<String,String> frame2 = DataFrame.ofObjects(rows, columns);
        final DataFrame<String,String> frame3 = DataFrame.ofObjects(rows, columns);
        final DataFrame<String,String> frame4 = DataFrame.ofObjects(rows, columns);
        source.applyValues(v -> "X: " + random.nextDouble() * 10d);
        source.forEachValue(v -> {
            frame1.data().setValue(v.rowOrdinal(), v.colOrdinal(), source.data().getValue(v.rowOrdinal(), v.colOrdinal()));
            frame2.data().setValue(v.rowKey(), v.colOrdinal(), source.data().getValue(v.rowOrdinal(), v.colOrdinal()));
            frame3.data().setValue(v.rowOrdinal(), v.colKey(), source.data().getValue(v.rowOrdinal(), v.colOrdinal()));
            frame4.data().setValue(v.rowKey(), v.colKey(), source.data().getValue(v.rowOrdinal(), v.colOrdinal()));
        });
        DataFrameAsserts.assertEqualsByIndex(source, frame1);
        DataFrameAsserts.assertEqualsByIndex(source, frame2);
        DataFrameAsserts.assertEqualsByIndex(source, frame3);
        DataFrameAsserts.assertEqualsByIndex(source, frame4);
    }



    @Test(dataProvider="exceptions", expectedExceptions={DataFrameException.class})
    public void testExceptionOnBooleanRead(int scenario) {
        final Random random = new Random(2344);
        final Range<String> rows = Range.of(0, 100).map(i -> "R" + i);
        final Range<String> columns = Range.of(0, 100).map(i -> "C" + i);
        final DataFrame<String,String> source = DataFrame.ofBooleans(rows, columns);
        source.applyBooleans(v -> random.nextBoolean());
        switch (scenario) {
            case 0: source.data().getBoolean(1000, 50);    break;
            case 1: source.data().getBoolean(50, 1000);    break;
            case 2: source.data().getBoolean("X", 50);     break;
            case 3: source.data().getBoolean("R10", 500);  break;
            case 4: source.data().getBoolean(50, "Y");     break;
            case 5: source.data().getBoolean(500, "C10");  break;
            case 6: source.data().getBoolean("R10", "Y");  break;
            case 7: source.data().getBoolean("X", "C10");  break;
            default:    throw new IllegalArgumentException("Unsupported scenario code: " + scenario);
        }
    }


    @Test(dataProvider="exceptions", expectedExceptions={DataFrameException.class})
    public void testExceptionOnIntReads(int scenario) {
        final Random random = new Random(2344);
        final Range<String> rows = Range.of(0, 100).map(i -> "R" + i);
        final Range<String> columns = Range.of(0, 100).map(i -> "C" + i);
        final DataFrame<String,String> source = DataFrame.ofInts(rows, columns);
        source.applyInts(v -> random.nextInt());
        switch (scenario) {
            case 0: source.data().getInt(1000, 50);    break;
            case 1: source.data().getInt(50, 1000);    break;
            case 2: source.data().getInt("X", 50);     break;
            case 3: source.data().getInt("R10", 500);  break;
            case 4: source.data().getInt(50, "Y");     break;
            case 5: source.data().getInt(500, "C10");  break;
            case 6: source.data().getInt("R10", "Y");  break;
            case 7: source.data().getInt("X", "C10");  break;
            default:    throw new IllegalArgumentException("Unsupported scenario code: " + scenario);
        }
    }


    @Test(dataProvider="exceptions", expectedExceptions={DataFrameException.class})
    public void testExceptionOnLongReads(int scenario) {
        final Random random = new Random(2344);
        final Range<String> rows = Range.of(0, 100).map(i -> "R" + i);
        final Range<String> columns = Range.of(0, 100).map(i -> "C" + i);
        final DataFrame<String,String> source = DataFrame.ofLongs(rows, columns);
        source.applyLongs(v -> random.nextLong());
        switch (scenario) {
            case 0: source.data().getLong(1000, 50);    break;
            case 1: source.data().getLong(50, 1000);    break;
            case 2: source.data().getLong("X", 50);     break;
            case 3: source.data().getLong("R10", 500);  break;
            case 4: source.data().getLong(50, "Y");     break;
            case 5: source.data().getLong(500, "C10");  break;
            case 6: source.data().getLong("R10", "Y");  break;
            case 7: source.data().getLong("X", "C10");  break;
            default:    throw new IllegalArgumentException("Unsupported scenario code: " + scenario);
        }
    }


    @Test(dataProvider="exceptions", expectedExceptions={DataFrameException.class})
    public void testExceptionOnDoubleReads(int scenario) {
        final Random random = new Random(2344);
        final Range<String> rows = Range.of(0, 100).map(i -> "R" + i);
        final Range<String> columns = Range.of(0, 100).map(i -> "C" + i);
        final DataFrame<String,String> source = DataFrame.ofDoubles(rows, columns);
        source.applyDoubles(v -> random.nextDouble());
        switch (scenario) {
            case 0: source.data().getDouble(1000, 50);    break;
            case 1: source.data().getDouble(50, 1000);    break;
            case 2: source.data().getDouble("X", 50);     break;
            case 3: source.data().getDouble("R10", 500);  break;
            case 4: source.data().getDouble(50, "Y");     break;
            case 5: source.data().getDouble(500, "C10");  break;
            case 6: source.data().getDouble("R10", "Y");  break;
            case 7: source.data().getDouble("X", "C10");  break;
            default:    throw new IllegalArgumentException("Unsupported scenario code: " + scenario);
        }
    }


    @Test(dataProvider="exceptions", expectedExceptions={DataFrameException.class})
    public void testExceptionOnValueReads(int scenario) {
        final Random random = new Random(2344);
        final Range<String> rows = Range.of(0, 100).map(i -> "R" + i);
        final Range<String> columns = Range.of(0, 100).map(i -> "C" + i);
        final DataFrame<String,String> source = DataFrame.ofStrings(rows, columns);
        source.applyValues(v -> "X:" + random.nextDouble());
        switch (scenario) {
            case 0: source.data().getValue(1000, 50);    break;
            case 1: source.data().getValue(50, 1000);    break;
            case 2: source.data().getValue("X", 50);     break;
            case 3: source.data().getValue("R10", 500);  break;
            case 4: source.data().getValue(50, "Y");     break;
            case 5: source.data().getValue(500, "C10");  break;
            case 6: source.data().getValue("R10", "Y");  break;
            case 7: source.data().getValue("X", "C10");  break;
            default:    throw new IllegalArgumentException("Unsupported scenario code: " + scenario);
        }
    }


    @Test(dataProvider="exceptions", expectedExceptions={DataFrameException.class})
    public void testExceptionOnBooleanWrite(int scenario) {
        final Random random = new Random(2344);
        final Range<String> rows = Range.of(0, 100).map(i -> "R" + i);
        final Range<String> columns = Range.of(0, 100).map(i -> "C" + i);
        final DataFrame<String,String> source = DataFrame.ofBooleans(rows, columns);
        source.applyBooleans(v -> random.nextBoolean());
        switch (scenario) {
            case 0: source.data().setBoolean(1000, 50, random.nextBoolean());    break;
            case 1: source.data().setBoolean(50, 1000, random.nextBoolean());    break;
            case 2: source.data().setBoolean("X", 50, random.nextBoolean());     break;
            case 3: source.data().setBoolean("R10", 500, random.nextBoolean());  break;
            case 4: source.data().setBoolean(50, "Y", random.nextBoolean());     break;
            case 5: source.data().setBoolean(500, "C10", random.nextBoolean());  break;
            case 6: source.data().setBoolean("R10", "Y", random.nextBoolean());  break;
            case 7: source.data().setBoolean("X", "C10", random.nextBoolean());  break;
            default:    throw new IllegalArgumentException("Unsupported scenario code: " + scenario);
        }
    }


    @Test(dataProvider="exceptions", expectedExceptions={DataFrameException.class})
    public void testExceptionOnIntWrite(int scenario) {
        final Random random = new Random(2344);
        final Range<String> rows = Range.of(0, 100).map(i -> "R" + i);
        final Range<String> columns = Range.of(0, 100).map(i -> "C" + i);
        final DataFrame<String,String> source = DataFrame.ofInts(rows, columns);
        source.applyInts(v -> random.nextInt());
        switch (scenario) {
            case 0: source.data().setInt(1000, 50, random.nextInt());    break;
            case 1: source.data().setInt(50, 1000, random.nextInt());    break;
            case 2: source.data().setInt("X", 50, random.nextInt());     break;
            case 3: source.data().setInt("R10", 500, random.nextInt());  break;
            case 4: source.data().setInt(50, "Y", random.nextInt());     break;
            case 5: source.data().setInt(500, "C10", random.nextInt());  break;
            case 6: source.data().setInt("R10", "Y", random.nextInt());  break;
            case 7: source.data().setInt("X", "C10", random.nextInt());  break;
            default:    throw new IllegalArgumentException("Unsupported scenario code: " + scenario);
        }
    }


    @Test(dataProvider="exceptions", expectedExceptions={DataFrameException.class})
    public void testExceptionOnLongWrite(int scenario) {
        final Random random = new Random(2344);
        final Range<String> rows = Range.of(0, 100).map(i -> "R" + i);
        final Range<String> columns = Range.of(0, 100).map(i -> "C" + i);
        final DataFrame<String,String> source = DataFrame.ofLongs(rows, columns);
        source.applyLongs(v -> random.nextLong());
        switch (scenario) {
            case 0: source.data().setLong(1000, 50, random.nextLong());    break;
            case 1: source.data().setLong(50, 1000, random.nextLong());    break;
            case 2: source.data().setLong("X", 50, random.nextLong());     break;
            case 3: source.data().setLong("R10", 500, random.nextLong());  break;
            case 4: source.data().setLong(50, "Y", random.nextLong());     break;
            case 5: source.data().setLong(500, "C10", random.nextLong());  break;
            case 6: source.data().setLong("R10", "Y", random.nextLong());  break;
            case 7: source.data().setLong("X", "C10", random.nextLong());  break;
            default:    throw new IllegalArgumentException("Unsupported scenario code: " + scenario);
        }
    }


    @Test(dataProvider="exceptions", expectedExceptions={DataFrameException.class})
    public void testExceptionOnDoubleWrite(int scenario) {
        final Random random = new Random(2344);
        final Range<String> rows = Range.of(0, 100).map(i -> "R" + i);
        final Range<String> columns = Range.of(0, 100).map(i -> "C" + i);
        final DataFrame<String,String> source = DataFrame.ofDoubles(rows, columns);
        source.applyDoubles(v -> random.nextDouble());
        switch (scenario) {
            case 0: source.data().setDouble(1000, 50, random.nextDouble());    break;
            case 1: source.data().setDouble(50, 1000, random.nextDouble());    break;
            case 2: source.data().setDouble("X", 50, random.nextDouble());     break;
            case 3: source.data().setDouble("R10", 500, random.nextDouble());  break;
            case 4: source.data().setDouble(50, "Y", random.nextDouble());     break;
            case 5: source.data().setDouble(500, "C10", random.nextDouble());  break;
            case 6: source.data().setDouble("R10", "Y", random.nextDouble());  break;
            case 7: source.data().setDouble("X", "C10", random.nextDouble());  break;
            default:    throw new IllegalArgumentException("Unsupported scenario code: " + scenario);
        }
    }


    @Test(dataProvider="exceptions", expectedExceptions={DataFrameException.class})
    public void testExceptionOnValueWrite(int scenario) {
        final Random random = new Random(2344);
        final Range<String> rows = Range.of(0, 100).map(i -> "R" + i);
        final Range<String> columns = Range.of(0, 100).map(i -> "C" + i);
        final DataFrame<String,String> source = DataFrame.ofStrings(rows, columns);
        source.applyValues(v -> "X:" + random.nextDouble());
        switch (scenario) {
            case 0: source.data().setValue(1000, 50, "?");    break;
            case 1: source.data().setValue(50, 1000, "?");    break;
            case 2: source.data().setValue("X", 50, "?");     break;
            case 3: source.data().setValue("R10", 500, "?");  break;
            case 4: source.data().setValue(50, "Y", "?");     break;
            case 5: source.data().setValue(500, "C10", "?");  break;
            case 6: source.data().setValue("R10", "Y", "?");  break;
            case 7: source.data().setValue("X", "C10", "?");  break;
            default:    throw new IllegalArgumentException("Unsupported scenario code: " + scenario);
        }
    }

    @Test(dataProvider="args2", expectedExceptions={DataFrameException.class})
    public void testExceptionOnReadWrongTypeWhenBoolean(int scenario) {
        final DataFrame<String,String> frame = TestDataFrames.random(boolean.class, 10, 10);
        switch (scenario) {
            case 0: frame.data().getDouble(0, 0);  break;
            case 1: frame.data().getInt(0,0);      break;
            case 2: frame.data().getLong(0,0);     break;
        }
    }

    @Test(dataProvider="args3", expectedExceptions={DataFrameException.class})
    public void testExceptionOnReadWrongTypeWhenInt(int scenario) {
        final DataFrame<String,String> frame = TestDataFrames.random(int.class, 10, 10);
        switch (scenario) {
            case 0: frame.data().getBoolean(0, 0); break;
            case 1: frame.data().getBoolean(0,0);  break;
        }
    }

    @Test(dataProvider="args3", expectedExceptions={DataFrameException.class})
    public void testExceptionOnReadWrongTypeWhenLong(int scenario) {
        final DataFrame<String,String> frame = TestDataFrames.random(long.class, 10, 10);
        switch (scenario) {
            case 0: frame.data().getBoolean(0,0);  break;
            case 1: frame.data().getInt(0, 0);      break;
        }
    }

    @Test(dataProvider="args2", expectedExceptions={DataFrameException.class})
    public void testExceptionOnReadWrongTypeWhenDouble(int scenario) {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 10, 10);
        switch (scenario) {
            case 0: frame.data().getBoolean(0, 0);  break;
            case 1: frame.data().getInt(0, 0);      break;
            case 2: frame.data().getLong(0, 0);     break;
        }
    }

    @Test(dataProvider="args2", expectedExceptions={DataFrameException.class})
    public void testExceptionOnWriteWrongTypeWhenBoolean(int scenario) {
        final DataFrame<String,String> frame = TestDataFrames.random(boolean.class, 10, 10);
        switch (scenario) {
            case 0: frame.data().setDouble(0, 0, Double.NaN);    break;
            case 1: frame.data().setInt(0, 0, 1);                break;
            case 2: frame.data().setLong(0, 0, 2);               break;
        }
    }

    @Test(dataProvider="args3", expectedExceptions={DataFrameException.class})
    public void testExceptionOnWriteWrongTypeWhenInt(int scenario) {
        final DataFrame<String,String> frame = TestDataFrames.random(int.class, 10, 10);
        switch (scenario) {
            case 0: frame.data().setBoolean(0, 0, false);      break;
            case 1: frame.data().setLong(0, 0, 0L);            break;
        }
    }

    @Test(dataProvider="args3", expectedExceptions={DataFrameException.class})
    public void testExceptionOnWriteWrongTypeWhenLong(int scenario) {
        final DataFrame<String,String> frame = TestDataFrames.random(long.class, 10, 10);
        switch (scenario) {
            case 0: frame.data().setBoolean(0, 0, true);     break;
            case 1: frame.data().setInt(0, 0, 8);            break;
        }
    }

    @Test(dataProvider="args2", expectedExceptions={DataFrameException.class})
    public void testExceptionOnWriteWrongTypeWhenDouble(int scenario) {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 10, 10);
        switch (scenario) {
            case 0: frame.data().setBoolean(0, 0, true); break;
            case 1: frame.data().setInt(0, 0, 9);        break;
            case 2: frame.data().setLong(0, 0, 8L);      break;
        }
    }

    @Test(dataProvider= "args4")
    public void testDataFrameValueRead(Class<?> type) {
        final DataFrame<String,String> source = TestDataFrames.random(type, 100, 100);
        final DataFrame<String,String> target = source.copy().applyValues(v -> null);
        if (type == boolean.class) {
            source.forEachValue(v -> target.data().setBoolean(v.rowOrdinal(), v.colOrdinal(), v.getBoolean()));
        } else if (type == int.class) {
            source.forEachValue(v -> target.data().setInt(v.rowOrdinal(), v.colOrdinal(), v.getInt()));
        } else if (type == long.class) {
            source.forEachValue(v -> target.data().setLong(v.rowOrdinal(), v.colOrdinal(), v.getLong()));
        } else if (type == double.class) {
            source.forEachValue(v -> target.data().setDouble(v.rowOrdinal(), v.colOrdinal(), v.getDouble()));
        } else {
            source.forEachValue(v -> target.data().setValue(v.rowOrdinal(), v.colOrdinal(), v.getValue()));
        }
        DataFrameAsserts.assertEqualsByIndex(source, target);
    }


    @Test(dataProvider= "args4")
    public void testDataFrameValueWrite(Class<?> type) {
        final DataFrame<String,String> source = TestDataFrames.random(type, 100, 100);
        final DataFrame<String,String> target = source.copy().applyValues(v -> null);
        if (type == boolean.class) {
            target.forEachValue(v -> v.setBoolean(source.data().getBoolean(v.rowOrdinal(), v.colOrdinal())));
        } else if (type == int.class) {
            target.forEachValue(v -> v.setInt(source.data().getInt(v.rowOrdinal(), v.colOrdinal())));
        } else if (type == long.class) {
            target.forEachValue(v -> v.setLong(source.data().getLong(v.rowOrdinal(), v.colOrdinal())));
        } else if (type == double.class) {
            target.forEachValue(v -> v.setDouble(source.data().getDouble(v.rowOrdinal(), v.colOrdinal())));
        } else {
            target.forEachValue(v -> v.setValue(source.data().getValue(v.rowOrdinal(), v.colOrdinal())));
        }
        DataFrameAsserts.assertEqualsByIndex(source, target);
    }



    static class Coordinate {

        private int rowIndex;
        private int colIndex;

        Coordinate(int rowIndex, int colIndex) {
            this.rowIndex = rowIndex;
            this.colIndex = colIndex;
        }

        @Override()
        public int hashCode() {
            return Arrays.hashCode(new int[] {rowIndex, colIndex});
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Coordinate && (((Coordinate)other).rowIndex == this.rowIndex && ((Coordinate)other).colIndex == this.colIndex);
        }
    }
}
