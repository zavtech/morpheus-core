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

import java.util.Random;
import java.util.stream.IntStream;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Units tests for the Array update functions.
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class ArrayUpdateTests {


    @DataProvider(name = "types")
    public Object[][] types() {
        return new ArraysBasicTests().types();
    }


    @Test(dataProvider = "types")
    public <T> void testConcatenate(Class<T> type, ArrayStyle style) {
        final Array<T> array1 = ArraysBasicTests.createRandomArray(type, 5000, style);
        final Array<T> array2 = ArraysBasicTests.createRandomArray(type, 5000, style);
        final Array<T> array3 = array1.concat(array2);
        Assert.assertEquals(array3.length(), array1.length() + array2.length());
        for (int i=0; i<array1.length(); ++i) {
            Assert.assertEquals(array3.getValue(i), array1.getValue(i));
        }
        for (int i=0; i<array2.length(); ++i) {
            Assert.assertEquals(array3.getValue(i + array1.length()), array2.getValue(i));
        }
    }

    @Test(dataProvider = "types")
    public <T> void testArrayUpdate(Class<T> type, ArrayStyle style) {
        final Random random = new Random();
        final Array<T> array1 = ArraysBasicTests.createRandomArray(type, 5000, style).shuffle(2);
        final Array<T> array2 = ArraysBasicTests.createRandomArray(type, 5000, style).shuffle(2);
        final int[] ints1 = IntStream.range(0, 20).map(i -> random.nextInt(5000)).distinct().toArray();
        final int[] ints2 = IntStream.range(0, 20).map(i -> random.nextInt(5000)).distinct().toArray();
        final int count = Math.min(ints1.length, ints2.length);
        final int[] fromIndexes = new int[count];
        final int[] toIndexes = new int[count];
        System.arraycopy(ints1, 0, fromIndexes, 0, count);
        System.arraycopy(ints2, 0, toIndexes, 0, count);
        Assert.assertFalse(array1.equals(array2), "The two arrays are not equal");
        array1.update(array2, fromIndexes, toIndexes);
        Assert.assertFalse(array1.equals(array2), "The two arrays are not equal");
        for (int i=0; i<fromIndexes.length; ++i) {
            final T v1 = array2.getValue(ints1[i]);
            final T v2 = array1.getValue(ints2[i]);
            Assert.assertEquals(v1, v2, "The values match after update");
        }
    }
}
