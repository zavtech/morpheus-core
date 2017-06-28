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

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.array.ArrayType;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.range.Range;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * A unit test of the equals() method on various forms of DataFrame
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class EqualsTest {


    @DataProvider(name = "testArgs")
    public Object[][] testArgs() {
        return new Object[][]{
            { boolean.class },
            { boolean.class },
            { int.class },
            { long.class },
            { double.class },
            { String.class },
        };
    }


    @Test(dataProvider="testArgs")
    public void testEquals(Class<?> type) {
        final DataFrame<String,String> frame = TestDataFrames.random(type, 100, 100);
        final DataFrame<String,String> copy = frame.copy();
        Assert.assertTrue(frame.equals(copy), "The frames are equal");
    }


    @Test(dataProvider="testArgs")
    public void testNotEquals1(Class type) {
        final DataFrame<String,String> frame = TestDataFrames.random(type, 100, 100);
        final DataFrame<String,String> copy = frame.copy();
        switch (ArrayType.of(type)) {
            case BOOLEAN:   copy.data().setBoolean(5, 5, !frame.data().getBoolean(5, 5));   break;
            case INTEGER:   copy.data().setInt(5, 5, frame.data().getInt(5, 5) + 10);       break;
            case LONG:      copy.data().setLong(5, 5, frame.data().getLong(5, 5) + 10L);    break;
            case DOUBLE:    copy.data().setDouble(5, 5, 10d);                               break;
            case STRING:    copy.data().setValue(5, 5, "ABC");                              break;
            case OBJECT:    copy.data().setValue(5, 5, "XYZ");                              break;
            default:    throw new IllegalArgumentException("Unsupported type: " + type);
        }
        Assert.assertTrue(!frame.equals(copy), "The frames are equal");
    }


    @Test(dataProvider="testArgs")
    public void testNotEquals2(Class<?> type) {
        final DataFrame<String,String> frame = TestDataFrames.random(type, 100, 100);
        final DataFrame<String,String> copy = frame.copy();
        copy.rows().add("XYZ");
        Assert.assertTrue(!frame.equals(copy), "The frames are equal");
    }


    @Test(dataProvider="testArgs")
    public void testNotEquals3(Class<?> type) {
        final DataFrame<String,String> frame = TestDataFrames.random(type, 100, 100);
        final DataFrame<String,String> copy = frame.copy();
        copy.cols().add("ABC", Object.class);
        Assert.assertTrue(!frame.equals(copy), "The frames are equal");
    }


    @Test(dataProvider="testArgs")
    public void testNotEquals4(Class<?> type) {
        final DataFrame<String,String> frame = TestDataFrames.random(type, 100, 100);
        final DataFrame<String,String> copy = frame.copy();
        final DataFrame<String,String> copy2 = copy.rows().mapKeys(row -> row.key() + "_");
        Assert.assertTrue(!frame.equals(copy2), "The frames are equal");
    }


    @Test(dataProvider="testArgs")
    public void testNotEquals5(Class type) {
        final DataFrame<String,String> frame = TestDataFrames.random(type, 100, 100);
        final DataFrame<String,String> copy = frame.copy();
        final DataFrame<String,String> copy2 = copy.cols().mapKeys(col -> col.key() + "_");
        Assert.assertTrue(!frame.equals(copy2), "The frames are equal");
    }


    @Test()
    public void testMultipleTypeEquals() {
        final Random random = new Random(304);
        final Range<String> rowKeys = Range.of(0, 1000).map(i -> "R" + i);
        final Index<String> colKeys = Index.of(String.class, 10);
        final DataFrame<String,String> frame = DataFrame.ofObjects(rowKeys, colKeys);
        final int rowCount = frame.rowCount();
        frame.cols().add("C1", Array.of(Boolean.class, rowCount)).applyBooleans(v -> random.nextBoolean());
        frame.cols().add("C2", Array.of(Integer.class, rowCount)).applyInts(v -> random.nextInt());
        frame.cols().add("C3", Array.of(Long.class, rowCount)).applyLongs(v -> random.nextLong());
        frame.cols().add("C4", Array.of(Double.class, rowCount)).applyDoubles(v -> random.nextDouble());
        frame.cols().add("C5", Array.of(String.class, rowCount)).applyValues(v -> "X:" + random.nextDouble());
        final DataFrame<String,String> copy = frame.copy();
        Assert.assertTrue(frame.equals(copy), "The frames are equal");
    }
}
