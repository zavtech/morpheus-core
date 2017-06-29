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
package com.zavtech.morpheus.index;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.range.Range;

/**
 * Tests on Index sort capabilities.
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class IndexSortTests {


    @DataProvider(name="arrays")
    public Object[][] arrays() {
        final Object[][] arrays = new IndexBasicTests().arrays();
        for (int i=0; i<arrays.length; ++i) {
            final Array array = (Array)arrays[i][0];
            arrays[i][0] = shuffle(array);
        }
        return arrays;
    }

    @DataProvider(name="style")
    public Object[][] style() {
        return new Object[][] {
            { false }, { true }
        };
    }


    /**
     * Suffles the array to make random order
     * @param array the array reference
     * @param <T>   the array element type
     * @return      the shuffled array
     */
    private <T> Array<T> shuffle(Array<T> array) {
        final Random random = new Random();
        for (int i=0; i<5; ++i) {
            for (int j=0; j<array.length(); ++j) {
                final int k = random.nextInt(array.length());
                array.swap(j, k);
            }
        }
        return array;
    }


    @Test(dataProvider = "arrays")
    public <T extends Comparable> void testSort(Array<T> array) {
        Index<T> index = Index.of(array.copy());
        Assert.assertEquals(index.size(), array.length());
        Assert.assertFalse(isAscending(index, 0, index.size()), "The index is not sorted");
        Assert.assertFalse(isDescending(index, 0, index.size()), "The index is not sorted");

        index = index.sort(false, true);
        Assert.assertTrue(isAscending(index, 0, index.size()));
        for (int i = 0; i < array.length(); ++i) {
            final T key = array.getValue(i);
            Assert.assertEquals(index.getIndexForKey(key), i, "The index matches expected value");
        }

        index = index.sort(false, false);
        Assert.assertTrue(isDescending(index, 0, index.size()));
        for (int i = 0; i < array.length(); ++i) {
            final T key = array.getValue(i);
            Assert.assertEquals(index.getIndexForKey(key), i, "The index matches expected value");
        }

        index.resetOrder();
        for (int i = 0; i < array.length(); ++i) {
            final T key = array.getValue(i);
            Assert.assertEquals(index.getIndexForKey(key), i, "The index matches expected value");
            Assert.assertEquals(index.getOrdinalForKey(key), i, "The index matches expected value");
        }
    }


    @Test(dataProvider = "arrays")
    public <T extends Comparable> void testSortFilter(Array<T> array) {
        final Index<T> index = Index.of(array.copy());
        Index<T> filter = index.filter(array.copy(100, 500));
        Assert.assertEquals(index.size(), array.length());
        Assert.assertEquals(filter.size(), 400);
        Assert.assertFalse(isAscending(index, 0, index.size()), "The index is not sorted");
        Assert.assertFalse(isDescending(index, 0, index.size()), "The index is not sorted");
        Assert.assertFalse(isAscending(filter, 0, filter.size()), "The index is not sorted");
        Assert.assertFalse(isDescending(filter, 0, filter.size()), "The index is not sorted");

        filter = filter.sort(false, true);
        Assert.assertFalse(isAscending(index, 0, index.size()), "The index is not sorted");
        Assert.assertFalse(isDescending(index, 0, index.size()), "The index is not sorted");
        Assert.assertTrue(isAscending(filter, 0, filter.size()), "The filter is sorted");
        for (int i = 0; i < filter.size(); ++i) {
            final T key = filter.getKey(i);
            final int actual = filter.getIndexForKey(key);
            final int expected = index.getIndexForKey(key);
            Assert.assertEquals(actual, expected, "The index match for " + key);
        }
    }


    @Test(dataProvider = "arrays")
    public <T extends Comparable> void testPreviousKey(Array<T> array) {
        Index<T> index = Index.of(array.copy());
        Assert.assertFalse(isAscending(index, 0, index.size()), "The index is not sorted");
        Assert.assertFalse(isDescending(index, 0, index.size()), "The index is not sorted");
        index = index.sort(false, true);
        Assert.assertTrue(isAscending(index, 0, index.size()), "The index is not sorted");
        Assert.assertFalse(index.previousKey(index.first().get()).isPresent(), "No key previous to first");
        Assert.assertFalse(index.nextKey(index.last().get()).isPresent(), "No key next from last");

        for (int i=1; i<index.size()-1; ++i) {
            final T key = index.getKey(i);
            final T previous = index.getKey(i-1);
            final T next = index.getKey(i+1);
            Assert.assertEquals(index.previousKey(key).get(), previous, "Match to previous key");
            Assert.assertEquals(index.nextKey(key).get(), next, "Match to next key");
        }
    }



    @SuppressWarnings("unchecked")
    private boolean isAscending(Index<? extends Comparable> array, int start, int end) {
        Assert.assertTrue(array.size() > 1, "The array has elements");
        for (int i=start+1; i<end; ++i) {
            final Comparable v1 = array.getKey(i-1);
            final Comparable v2 = array.getKey(i);
            if (v1.compareTo(v2) > 0) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private boolean isDescending(Index<? extends Comparable> array, int start, int end) {
        Assert.assertTrue(array.size() > 1, "The array has elements");
        for (int i=start+1; i<end; ++i) {
            final Comparable v1 = array.getKey(i-1);
            final Comparable v2 = array.getKey(i);
            if (v1.compareTo(v2) < 0) {
                return false;
            }
        }
        return true;
    }


    @Test(dataProvider = "style")
    public void testIndexPerformance(boolean parallel) {
        final LocalDateTime start = LocalDateTime.now();
        final LocalDateTime end = start.plusSeconds(1000000);
        final Array<LocalDateTime> dates = Range.of(start, end, Duration.ofSeconds(1)).toArray().shuffle(3);
        final Index<LocalDateTime> index = Index.of(dates);
        final long t1 = System.nanoTime();
        final Index<LocalDateTime> sorted = index.sort(parallel, true);
        final long t2 = System.nanoTime();
        System.out.println("Sorted Index in " + ((t2-t1)/1000000 + " millis"));
        for (int j=1; j<index.size(); ++j) {
            final LocalDateTime d1 = sorted.getKey(j-1);
            final LocalDateTime d2 = sorted.getKey(j);
            if (d1.isAfter(d2)) {
                throw new RuntimeException("Index keys are not sorted");
            } else {
                final int i1 = index.getIndexForKey(d1);
                final int i2 = sorted.getIndexForKey(d1);
                if (i1 != i2) {
                    throw new RuntimeException("The indexes do not match between original and sorted");
                }
            }
        }
    }


}
