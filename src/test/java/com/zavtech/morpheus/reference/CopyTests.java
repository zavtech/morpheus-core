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

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.array.ArrayType;
import com.zavtech.morpheus.frame.DataFrameAsserts;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.util.PerfStat;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests the copy() method on various forms of a DataFrame
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class CopyTests {


    @DataProvider(name="flavours")
    public Object[][] getFlavours() {
        return new Object[][] {
                { boolean.class },
                { int.class },
                { long.class },
                { double.class },
                { String.class },
        };
    }

    @Test(dataProvider="flavours")
    public void testCopy(Class type) {
        final DataFrame<String,String> frame = TestDataFrames.random(type, 100, 100);
        final DataFrame<String,String> copy = frame.copy();
        Assert.assertTrue(frame != copy, "References are different");
        DataFrameAsserts.assertEqualsByIndex(frame, copy);
        switch (ArrayType.of(type)) {
            case BOOLEAN:   copy.applyBooleans(v -> !frame.data().getBoolean(v.rowOrdinal(), v.colOrdinal()));        break;
            case INTEGER:   copy.applyInts(v -> frame.data().getInt(v.rowOrdinal(), v.colOrdinal()) + 10);            break;
            case LONG:      copy.applyLongs(v -> frame.data().getLong(v.rowOrdinal(), v.colOrdinal()) + 10L);         break;
            case DOUBLE:    copy.applyDoubles(v -> Math.random() * 10d);                                              break;
            case STRING:    copy.applyValues(v -> "X" + frame.data().getValue(v.rowOrdinal(), v.colOrdinal()));        break;
            case OBJECT:    copy.applyValues(v -> "X" + frame.data().getValue(v.rowOrdinal(), v.colOrdinal()));    break;
        }
        frame.forEachValue(v -> {
            final Object v1 = v.getValue();
            final Object v2= copy.data().getValue(v.rowKey(), v.colKey());
            if (v1 != null && v2 != null) {
                Assert.assertTrue(!v1.equals(v2), "The values to not match at " + v1 + " != " + v2);
            } else if (v1 == null && v2 == null) {
                Assert.assertTrue(false, "The values to not match at " + v.rowKey() + ", " + v.colKey());
            }
        });
    }




    @Test(dataProvider="flavours")
    public <T> void testCopyExpansion(Class<T> type) {
        final DataFrame<String,String> frame = TestDataFrames.random(type, 100, 100);
        final DataFrame<String,String> copy = frame.copy();
        Assert.assertTrue(frame != copy, "References are different");
        DataFrameAsserts.assertEqualsByIndex(frame, copy);
        copy.rows().addAll(Arrays.asList("X", "Y", "Z"));
        copy.cols().add("A", Array.of(type, frame.rowCount()));
        copy.cols().add("B", Array.of(type, frame.rowCount()));
        copy.cols().add("C", Array.of(type, frame.rowCount()));
        Arrays.asList("X", "Y", "Z").forEach(key -> {
            Assert.assertTrue(copy.rows().contains(key), key + " exists in copy");
            Assert.assertTrue(!frame.rows().contains(key), key + " does not exist in original");
        });
        Arrays.asList("A", "B", "C").forEach(key -> {
            Assert.assertTrue(copy.cols().contains(key), key + " exists in copy");
            Assert.assertTrue(!frame.cols().contains(key), key + " does not exist in original");
        });
    }


    @Test()
    public void testCopyPerformance() {
        final Array<Integer> rowKeys = Range.of(0, 1000000).toArray();
        final Array<Integer> colKeys = Range.of(0, 10).toArray();
        final long t1 = System.currentTimeMillis();
        final XDataFrame<Integer,Integer> frame0 = (XDataFrame<Integer,Integer>)DataFrame.ofDoubles(rowKeys, colKeys).applyDoubles(v -> Math.random());
        final long t2 = System.currentTimeMillis();
        final XDataFrame<Integer,Integer> frame1 = (XDataFrame<Integer,Integer>)DataFrame.ofDoubles(rowKeys, colKeys).applyDoubles(v -> Math.random());
        final long t3 = System.currentTimeMillis();
        System.out.println("Frame 0 created in " + (t2-t1) + " millis");
        System.out.println("Frame 1 created in " + (t3-t2) + " millis");
        PerfStat.timeInMillis(5, () -> XDataFrameCopy.apply(frame0, frame1, rowKeys, colKeys)).print();
        DataFrameAsserts.assertEqualsByIndex(frame0, frame1);
    }

}
