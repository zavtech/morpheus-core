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
package com.zavtech.morpheus.array;

import java.util.Optional;

import com.zavtech.morpheus.util.Bounds;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests for the array range functionality
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class ArrayBoundsTests {


    @DataProvider(name = "types")
    public Object[][] types() {
        return new ArraysBasicTests().types();
    }


    @Test(dataProvider = "types")
    public <T> void testBounds(Class<T> type, ArrayStyle style) {
        final Array<T> array = ArraySortTests.random(type, 1000000, style);
        for (boolean parallel : new boolean[] { false, true }) {
            final Array<T> target = parallel ? array.parallel() : array.sequential();
            final T minValue = target.min().get();
            final T maxValue = target.max().get();
            final Bounds<T> bounds = target.bounds().get();
            Assert.assertTrue(bounds != null, "The range is not null");
            Assert.assertEquals(bounds.lower(), minValue, "Min value as expected");
            Assert.assertEquals(bounds.upper(), maxValue, "Max value as expected");
        }
    }

    @Test(dataProvider = "types")
    public <T> void testRangeOnEmpty(Class<T> type, ArrayStyle style) {
        final Array<T> array = ArraySortTests.random(type, 0, style);
        Assert.assertTrue(!array.bounds().isPresent());
    }

    @Test()
    public void testRangeWithNaNs() {
        final Array<Double> array = Array.of(Double.class, 100).applyDoubles(v -> Double.NaN);
        Assert.assertTrue(!array.bounds().isPresent());
    }

    @Test()
    public void testRangeWithNulls() {
        final Array<Object> array = Array.of(Object.class, 100);
        Assert.assertTrue(!array.bounds().isPresent());
    }


    @Test(dataProvider = "types")
    public <T> void testMax(Class<T> type, ArrayStyle style) {
        final Array<T> array = ArraySortTests.random(type, 1000000, style);
        final Optional<T> max1 = array.sequential().max();
        final Optional<T> max2 = array.parallel().max();
        Assert.assertTrue(max1.isPresent());
        Assert.assertTrue(max2.isPresent());
        Assert.assertEquals(max1.get(), max2.get());
    }


    @Test(dataProvider = "types")
    public <T> void testMin(Class<T> type, ArrayStyle style) {
        final Array<T> array = ArraySortTests.random(type, 1000000, style);
        final Optional<T> min1 = array.sequential().min();
        final Optional<T> min2 = array.parallel().min();
        Assert.assertTrue(min1.isPresent());
        Assert.assertTrue(min2.isPresent());
        Assert.assertEquals(min1.get(), min2.get());
    }


}
