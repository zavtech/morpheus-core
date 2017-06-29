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

import java.util.Random;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.array.ArrayType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests for the update() method on DataFrame.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class UpdateTests {


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
    public void testUpdateValuesOnly(Class type) {
        final Random random = new Random();
        final DataFrame<String,String> frame1 = TestDataFrames.random(type, 10, 10);
        final DataFrame<String,String> frame2 =  TestDataFrames.random(type, 100, 100);
        Assert.assertTrue(frame2.rows().containsAll(frame1.rows().keyArray()));
        Assert.assertTrue(frame2.cols().containsAll(frame1.cols().keyArray()));
        switch (ArrayType.of(type)) {
            case BOOLEAN:   frame2.applyBooleans(v -> true);                          break;
            case INTEGER:   frame2.applyInts(v -> random.nextInt() * 10);             break;
            case LONG:      frame2.applyLongs(v -> random.nextLong() * 10);           break;
            case DOUBLE:    frame2.applyDoubles(v -> random.nextDouble() * 100);      break;
            case STRING:    frame2.applyValues(v -> "X:" + random.nextDouble());      break;
            case OBJECT:    frame2.applyValues(v -> "X:" + random.nextDouble());      break;
            default: throw new IllegalArgumentException("Unsupported type: " + type);
        }
        frame1.update(frame2, false, false);
        Assert.assertEquals(frame1.rowCount(), 10, "Row count is unchanged");
        Assert.assertEquals(frame1.colCount(), 10, "Columns count is unchanged");
        for (int i=0; i<frame1.rowCount(); ++i) {
            for (int j = 0; j<frame1.colCount(); ++j) {
                switch (ArrayType.of(type)) {
                    case BOOLEAN:   Assert.assertEquals(frame1.data().getBoolean(i,j), frame2.data().getBoolean(i,j));          break;
                    case INTEGER:   Assert.assertEquals(frame1.data().getInt(i,j), frame2.data().getInt(i,j));                  break;
                    case LONG:      Assert.assertEquals(frame1.data().getLong(i,j), frame2.data().getLong(i,j));                break;
                    case DOUBLE:    Assert.assertEquals(frame1.data().getDouble(i,j), frame2.data().getDouble(i,j));            break;
                    case STRING:    Assert.assertEquals((Object)frame1.data().getValue(i,j), frame2.data().getValue(i,j));       break;
                    case OBJECT:    Assert.assertEquals((Object)frame1.data().getValue(i,j), frame2.data().getValue(i,j));    break;
                    default:    throw new IllegalArgumentException("Unsupported type: " + type);
                }
            }
        }
    }


    @Test(dataProvider="frameTypes")
    public void testUpdateRowsAndValues(Class type) {
        final Random random = new Random();
        final DataFrame<String,String> frame1 = TestDataFrames.random(type, 10, 10);
        final DataFrame<String,String> frame2 =  TestDataFrames.random(type, 100, 100);
        Assert.assertTrue(frame2.rows().containsAll(frame1.rows().keyArray()));
        Assert.assertTrue(frame2.cols().containsAll(frame1.cols().keyArray()));
        switch (ArrayType.of(type)) {
            case BOOLEAN:   frame2.applyBooleans(v -> true);                          break;
            case INTEGER:   frame2.applyInts(v -> random.nextInt() * 10);             break;
            case LONG:      frame2.applyLongs(v -> random.nextLong() * 10);           break;
            case DOUBLE:    frame2.applyDoubles(v -> random.nextDouble() * 100);      break;
            case STRING:    frame2.applyValues(v -> "X:" + random.nextDouble());      break;
            case OBJECT:    frame2.applyValues(v -> "X:" + random.nextDouble());      break;
            default: throw new IllegalArgumentException("Unsupported type: " + type);
        }
        frame1.update(frame2, true, false);
        Assert.assertEquals(frame1.rowCount(), frame2.rowCount(), "Row count matches update frame");
        Assert.assertEquals(frame1.colCount(), 10, "Columns count is unchanged");
        for (int i=0; i<frame1.rowCount(); ++i) {
            for (int j = 0; j<frame1.colCount(); ++j) {
                switch (ArrayType.of(type)) {
                    case BOOLEAN:   Assert.assertEquals(frame1.data().getBoolean(i,j), frame2.data().getBoolean(i,j));        break;
                    case INTEGER:   Assert.assertEquals(frame1.data().getInt(i,j), frame2.data().getInt(i,j));                break;
                    case LONG:      Assert.assertEquals(frame1.data().getLong(i,j), frame2.data().getLong(i,j));              break;
                    case DOUBLE:    Assert.assertEquals(frame1.data().getDouble(i,j), frame2.data().getDouble(i,j));          break;
                    case STRING:    Assert.assertEquals((Object)frame1.data().getValue(i,j), frame2.data().getValue(i,j));    break;
                    case OBJECT:    Assert.assertEquals((Object)frame1.data().getValue(i,j), frame2.data().getValue(i,j));    break;
                    default:    throw new IllegalArgumentException("Unsupported type: " + type);
                }
            }
        }
    }


    @Test(dataProvider="frameTypes")
    public void testUpdateColumnsAndValues(Class type) {
        final Random random = new Random();
        final DataFrame<String,String> frame1 = TestDataFrames.random(type, 10, 10);
        final DataFrame<String,String> frame2 =  TestDataFrames.random(type, 100, 100);
        Assert.assertTrue(frame2.rows().containsAll(frame1.rows().keyArray()));
        Assert.assertTrue(frame2.cols().containsAll(frame1.cols().keyArray()));
        switch (ArrayType.of(type)) {
            case BOOLEAN:   frame2.applyBooleans(v -> true);                          break;
            case INTEGER:   frame2.applyInts(v -> random.nextInt() * 10);             break;
            case LONG:      frame2.applyLongs(v -> random.nextLong() * 10);           break;
            case DOUBLE:    frame2.applyDoubles(v -> random.nextDouble() * 100);      break;
            case STRING:    frame2.applyValues(v -> "X:" + random.nextDouble());      break;
            case OBJECT:    frame2.applyValues(v -> "X:" + random.nextDouble());      break;
            default: throw new IllegalArgumentException("Unsupported type: " + type);
        }
        frame1.update(frame2, false, true);
        Assert.assertEquals(frame1.rowCount(), 10, "Row count is unchanged");
        Assert.assertEquals(frame1.colCount(), frame2.colCount(), "Columns count matches update frame");
        for (int i=0; i<frame1.rowCount(); ++i) {
            for (int j = 0; j<frame1.colCount(); ++j) {
                switch (ArrayType.of(type)) {
                    case BOOLEAN:   Assert.assertEquals(frame1.data().getBoolean(i,j), frame2.data().getBoolean(i,j));        break;
                    case INTEGER:   Assert.assertEquals(frame1.data().getInt(i,j), frame2.data().getInt(i,j));                break;
                    case LONG:      Assert.assertEquals(frame1.data().getLong(i,j), frame2.data().getLong(i,j));              break;
                    case DOUBLE:    Assert.assertEquals(frame1.data().getDouble(i,j), frame2.data().getDouble(i,j));          break;
                    case STRING:    Assert.assertEquals((Object)frame1.data().getValue(i,j), frame2.data().getValue(i,j));    break;
                    case OBJECT:    Assert.assertEquals((Object)frame1.data().getValue(i,j), frame2.data().getValue(i,j));    break;
                    default:    throw new IllegalArgumentException("Unsupported type: " + type);
                }
            }
        }
    }


    @Test(dataProvider="frameTypes")
    public void testUpdateRowsColumnsAndValues(Class type) {
        final Random random = new Random();
        final DataFrame<String,String> frame1 = TestDataFrames.random(type, 10, 10);
        final DataFrame<String,String> frame2 =  TestDataFrames.random(type, 100, 100);
        Assert.assertTrue(frame2.rows().containsAll(frame1.rows().keyArray()));
        Assert.assertTrue(frame2.cols().containsAll(frame1.cols().keyArray()));
        switch (ArrayType.of(type)) {
            case BOOLEAN:   frame2.applyBooleans(v -> true);                          break;
            case INTEGER:   frame2.applyInts(v -> random.nextInt() * 10);             break;
            case LONG:      frame2.applyLongs(v -> random.nextLong() * 10);           break;
            case DOUBLE:    frame2.applyDoubles(v -> random.nextDouble() * 100);      break;
            case STRING:    frame2.applyValues(v -> "X:" + random.nextDouble());      break;
            case OBJECT:    frame2.applyValues(v -> "X:" + random.nextDouble());      break;
            default: throw new IllegalArgumentException("Unsupported type: " + type);
        }
        frame1.update(frame2, true, true);
        Assert.assertEquals(frame1.rowCount(), frame1.rowCount(), "Row count matches update frame");
        Assert.assertEquals(frame1.colCount(), frame2.colCount(), "Column count matches update frame");
        for (int i=0; i<frame1.rowCount(); ++i) {
            for (int j = 0; j<frame1.colCount(); ++j) {
                switch (ArrayType.of(type)) {
                    case BOOLEAN:   Assert.assertEquals(frame1.data().getBoolean(i,j), frame2.data().getBoolean(i,j));        break;
                    case INTEGER:   Assert.assertEquals(frame1.data().getInt(i,j), frame2.data().getInt(i,j));                break;
                    case LONG:      Assert.assertEquals(frame1.data().getLong(i,j), frame2.data().getLong(i,j));              break;
                    case DOUBLE:    Assert.assertEquals(frame1.data().getDouble(i,j), frame2.data().getDouble(i,j));          break;
                    case STRING:    Assert.assertEquals(frame1.data().getDouble(i,j), frame2.data().getDouble(i,j));          break;
                    case OBJECT:    Assert.assertEquals((Object)frame1.data().getValue(i,j), frame2.data().getValue(i,j));    break;
                    default:    throw new IllegalArgumentException("Unsupported type: " + type);
                }
            }
        }
    }


    @Test(dataProvider="frameTypes", expectedExceptions={DataFrameException.class})
    public void testUpdateRowsColumnsOnFilterFails(Class type) {
        final Random random = new Random();
        final DataFrame<String,String> frame1 = TestDataFrames.random(type, 10, 10).select(rowKey -> true, colKey -> true);
        final DataFrame<String,String> frame2 =  TestDataFrames.random(type, 100, 100);
        Assert.assertTrue(frame2.rows().containsAll(frame1.rows().keyArray()));
        Assert.assertTrue(frame2.cols().containsAll(frame1.cols().keyArray()));
        switch (ArrayType.of(type)) {
            case BOOLEAN:   frame2.applyBooleans(v -> true);                          break;
            case INTEGER:   frame2.applyInts(v -> random.nextInt() * 10);             break;
            case LONG:      frame2.applyLongs(v -> random.nextLong() * 10);           break;
            case DOUBLE:    frame2.applyDoubles(v -> random.nextDouble() * 100);      break;
            case STRING:    frame2.applyValues(v -> "X:" + random.nextDouble());      break;
            case OBJECT:    frame2.applyValues(v -> "X:" + random.nextDouble());      break;
            default: throw new IllegalArgumentException("Unsupported type: " + type);
        }
        frame1.update(frame2, true, true);
    }


}
