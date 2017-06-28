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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Optional;

import com.zavtech.morpheus.array.ArrayType;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameValue;
import com.zavtech.morpheus.util.Bounds;
import com.zavtech.morpheus.range.Range;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests for functions that generate upper and lower bounds information on a DataFrame.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class BoundsTests {

    @DataProvider(name="SparseOrDense")
    public Object[][] getSparseOrDense() {
        return new Object[][] {
                { false },
                { true }
        };
    }

    @DataProvider(name="flavours")
    public Object[][] flavours() {
        return new Object[][] {
                { int.class },
                { long.class },
                { double.class },
                { String.class },
                { Date.class },
                { LocalDate.class },
                { LocalTime.class },
                { LocalDateTime.class },
                { ZonedDateTime.class },
        };
    }


    @Test(dataProvider = "flavours")
    public <T> void testBoundsWithNulls(Class<T> type) {
        final ArrayType arrayType = ArrayType.of(type);
        if (!arrayType.isInteger() && !arrayType.isLong()) {
            final Range<Integer> rowKeys = Range.of(0, 100);
            final Range<Integer> colKeys = Range.of(0, 100);
            final DataFrame<Integer,Integer> frame = DataFrame.of(rowKeys, colKeys, type);
            frame.rows().forEach(row -> Assert.assertTrue(!row.bounds().isPresent()));
            frame.cols().forEach(col -> Assert.assertTrue(!col.bounds().isPresent()));
        }
    }


    @Test(dataProvider = "flavours")
    public <T> void testRowBounds(Class<T> type) {
        final ArrayType arrayType = ArrayType.of(type);
        final DataFrame<String,String> frame = TestDataFrames.random(type, 100, 100);
        frame.rows().forEach(row -> {
            final Optional<Bounds<Comparable>> bounds = row.bounds();
            Assert.assertTrue(bounds.isPresent(), "Bounds exist");
            final Comparable min = row.min(v -> !v.isNull()).map(v -> (Comparable)v.getValue()).orElse(null);
            final Comparable max = row.max(v -> !v.isNull()).map(v -> (Comparable)v.getValue()).orElse(null);
            final Comparable lower = bounds.get().lower();
            final Comparable upper = bounds.get().upper();
            Assert.assertEquals(lower, min, "Lower bounds matches min value");
            Assert.assertEquals(upper, max, "Upper bounds matches max value");
            if (arrayType.isNumeric()) {
                final double minValue = row.stats().min();
                final double maxValue = row.stats().max();
                Assert.assertEquals(((Number)lower).doubleValue(), minValue, "Lower bounds matches min stats value");
                Assert.assertEquals(((Number)upper).doubleValue(), maxValue, "Upper bounds matches max stats value");
            }
        });
    }


    @Test()
    public void testColumnBounds() {
        final DataFrame<Integer,String> frame = TestDataFrames.createMixedRandomFrame(Integer.class, 1000);
        frame.cols().forEach(column -> {
            final Optional<Bounds<Comparable>> bounds = column.bounds();
            Assert.assertTrue(bounds.isPresent(), "Bounds exist");
            final Comparable min = column.min().map(v -> (Comparable)v.getValue()).orElse(null);
            final Comparable max = column.max().map(v -> (Comparable)v.getValue()).orElse(null);
            final Comparable lower = bounds.get().lower();
            final Comparable upper = bounds.get().upper();
            Assert.assertEquals(lower, min, "Lower bounds matches min value");
            Assert.assertEquals(upper, max, "Upper bounds matches max value");
            if (ArrayType.of(column.typeInfo()).isNumeric()) {
                final double minValue = column.stats().min();
                final double maxValue = column.stats().max();
                Assert.assertEquals(((Number)lower).doubleValue(), minValue, "Lower bounds matches min stats value");
                Assert.assertEquals(((Number)upper).doubleValue(), maxValue, "Upper bounds matches max stats value");
            }
        });
    }
}
