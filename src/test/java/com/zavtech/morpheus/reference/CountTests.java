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

import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameValue;

/**
 * Unit test for various count functions
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class CountTests {


    @DataProvider(name="parallel")
    public Object[][] parallel() {
        return new Object[][] { {false}, {true} };
    }


    @Test(dataProvider = "parallel")
    public void testFrameValueCount(boolean parallel) {
        DataFrame<String,String> frame = TestDataFrames.random(Double.class, 10000, 10);
        AtomicInteger expected = new AtomicInteger();
        frame.forEachValue(v -> {
            if (v.isNull()) {
                expected.incrementAndGet();
            }
        });
        DataFrame<String,String> target = parallel ? frame.parallel() : frame.sequential();
        int actual = frame.count(DataFrameValue::isNull);
        Assert.assertEquals(actual, expected.get(), "Conditional counts match");
        Assert.assertEquals(100000, frame.count(v -> true), "Unconditional counts match");
    }


    @Test(dataProvider = "parallel")
    public void testRowValueCount(boolean parallel) {
        DataFrame<String,String> frame = TestDataFrames.random(Double.class, 10000, 10);
        DataFrame<String,String> target = parallel ? frame.parallel() : frame.sequential();
        target.rows().forEach(row -> {
            AtomicInteger expected = new AtomicInteger();
            row.forEachValue(v -> {
                if (v.isNull()) {
                    expected.incrementAndGet();
                }
            });
            int actual = row.count(DataFrameValue::isNull);
            Assert.assertEquals(actual, expected.get(), "Conditional counts match");
            Assert.assertEquals(10, row.size(), "Unconditional counts match");
        });
    }


    @Test(dataProvider = "parallel")
    public void testColumnValueCount(boolean parallel) {
        DataFrame<String,String> frame = TestDataFrames.random(Double.class, 10000, 10);
        DataFrame<String,String> target = parallel ? frame.parallel() : frame.sequential();
        target.cols().forEach(column -> {
            AtomicInteger expected = new AtomicInteger();
            column.forEachValue(v -> {
                if (v.isNull()) {
                    expected.incrementAndGet();
                }
            });
            int actual = column.count(DataFrameValue::isNull);
            Assert.assertEquals(actual, expected.get(), "Conditional counts match");
            Assert.assertEquals(10000, column.size(), "Unconditional counts match");
        });
    }



}
