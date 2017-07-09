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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.IntStream;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBuilder;
import com.zavtech.morpheus.array.ArrayUtils;
import com.zavtech.morpheus.array.ArrayValue;
import com.zavtech.morpheus.util.Predicates;
import com.zavtech.morpheus.range.Range;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests for the Index class
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class IndexBasicTests {


    @DataProvider(name="arrays")
    public Object[][] arrays() {
        return new Object[][] {
            { Range.of(2000, 5000).toArray() },
            { Range.of(2000L, 5000L).toArray() },
            { Range.of(2000d, 5000d).toArray() },
            { Range.of(1000, 3000).map(i -> "String-" + i).toArray() },
            { Range.of(LocalDate.now(), LocalDate.now().plusDays(2000)).toArray() },
            { Range.of(LocalTime.of(0,0), LocalTime.of(12, 0), Duration.ofSeconds(1)).toArray() },
            { Range.of(LocalDateTime.now(), LocalDateTime.now().plusDays(2), Duration.ofMinutes(1)).toArray() },
            { Range.of(ZonedDateTime.now(), ZonedDateTime.now().plusDays(2), Duration.ofMinutes(1)).toArray() },
            { Array.of(Range.of(2000d, 5000d).toArray().stream().values().toArray()) },
        };
    }


    @DataProvider(name="indexes")
    public Object[][] indexes() {
        return new Object[][] {
                { Index.of(Integer.class, 1000), Range.of(2000, 5000).toArray() },
                { Index.of(Long.class, 1000), Range.of(2000L, 5000L).toArray() },
                { Index.of(Double.class, 1000), Range.of(2000d, 5000d).toArray() },
                { Index.of(String.class, 1000), Range.of(1000, 3000).map(i -> "String-" + i).toArray() },
                { Index.of(LocalDate.class, 1000), Range.of(LocalDate.now(), LocalDate.now().plusDays(2000)).toArray() },
                { Index.of(LocalTime.class, 1000), Range.of(LocalTime.of(0,0), LocalTime.of(12, 0), Duration.ofSeconds(1)).toArray() },
                { Index.of(LocalDateTime.class, 1000), Range.of(LocalDateTime.now(), LocalDateTime.now().plusDays(2), Duration.ofMinutes(1)).toArray() },
                { Index.of(ZonedDateTime.class, 1000), Range.of(ZonedDateTime.now(), ZonedDateTime.now().plusDays(2), Duration.ofMinutes(1)).toArray() },
                { Index.of(Object.class, 1000), Range.of(1000, 4000).map( i -> {
                    Object key = null;
                    switch (i % 10) {
                        case 0: key = LocalDate.now().plusDays(i);  break;
                        case 1: key = LocalTime.of(12,0).plusNanos(i);  break;
                        case 2: key = new Long(i);      break;
                        case 3: key = new Double(i);    break;
                        default: key = i;  break;
                    }
                    return key;
                }).toArray() }
        };
    }


    @Test(dataProvider = "indexes")
    public <T> void testIndexBasics(Index<T> index, Array<T> array) {
        Assert.assertEquals(index.type(), array.type());
        for (int i=0; i<1000; ++i) index.add(array.getValue(i));
        Assert.assertEquals(index.size(), 1000);
        index.addAll(array.copy(1000, array.length()), true);
        Assert.assertEquals(index.size(), array.length());
        Assert.assertEquals(array.first(v -> true).map(ArrayValue::getValue).get(), index.first().get(), "First key match");
        Assert.assertEquals(array.last(v -> true).map(ArrayValue::getValue).get(), index.last().get(), "Last key match");
        Assert.assertEquals(index.isFilter(), false, "Index is not a filter");
        array.forEachValue(v -> {
            final int i = v.index();
            final T actual = index.getKey(i);
            final T expected = array.getValue(i);
            Assert.assertEquals(actual, expected, "Values match at " + i);
            Assert.assertEquals(index.getIndexForKey(expected), i, "The index matches for " + i);
            Assert.assertEquals(index.getOrdinalForKey(expected), i, "The ordinal matches for " + i);
            Assert.assertEquals(index.getIndexForOrdinal(i), i, "The index matches ordinal in unsorted index");
        });
        Assert.assertEquals(index.toArray(), array, "Index yields expected array of keys");
        Assert.assertEquals(index.toArray(28, 822), array.copy(28, 822), "Index yields expected array of keys");
        Assert.assertEquals(index.keys().collect(ArrayUtils.toArray(array.length())), array, "Index yields expected array of keys");
        Assert.assertTrue(Arrays.equals(index.indexes().toArray(), index.ordinals(array).toArray()), "Index match ordinals in unsorted index");
        Assert.assertTrue(Arrays.equals(index.indexes(array).toArray(), index.ordinals(array).toArray()), "Index match ordinals in unsorted index");
        Assert.assertTrue(index.containsAll(array), "Index conntains all expected keys");
        Assert.assertEquals(toArray(index.iterator()), array, "The array from iterator matches source array");
        assertEquals(index, index.copy());
        final ArrayBuilder<T> builder = ArrayBuilder.of(1000);
        index.forEachEntry((T key, int i) -> builder.add(key));
        Assert.assertEquals(builder.toArray(), array, "The array from iterator matches source array");
    }


    @Test(dataProvider = "arrays")
    public <T> void replace(Array<T> array) {
        final int midPoint = array.length() / 2;
        final Index<T> index = Index.of(array.copy(0, midPoint));
        Assert.assertEquals(index.size(), midPoint, "Index size as expected");
        for (int i=0; i<midPoint; ++i) {
            final T expected = array.getValue(i);
            final T actual = index.getKey(i);
            Assert.assertEquals(actual, expected, "Keys match for ordinal " + i);
        }
        final Map<T,Integer> indexMap = new HashMap<>();
        final Map<T,Integer> ordinalMap = new HashMap<>();
        for (int i=0; i<index.size(); ++i) {
            final T toReplace = index.getKey(i);
            final T replacement = array.getValue(i+midPoint);
            indexMap.put(replacement, index.getIndexForKey(toReplace));
            ordinalMap.put(replacement, index.getOrdinalForKey(toReplace));
            index.replace(toReplace, replacement);
        }
        for (int i=0; i<midPoint; ++i) {
            final T expected = array.getValue(i+midPoint);
            final T actual = index.getKey(i);
            Assert.assertEquals(actual, expected, "Keys match for ordinal " + i);
            Assert.assertEquals(index.getIndexForKey(actual), indexMap.get(actual).intValue(), "Index matches");
            Assert.assertEquals(index.getOrdinalForKey(actual), ordinalMap.get(actual).intValue(), "Index matches");
        }
    }


    @Test(dataProvider = "arrays", expectedExceptions = { IndexException.class })
    public <T> void replaceFailsIfSourceKeyDoesNotExist(Array<T> array) {
        final Index<T> index = Index.of(array.copy(0, array.length() / 2));
        index.replace(array.last(v -> true).map(ArrayValue::getValue).get(), array.getValue(array.length()-2));
    }


    @Test(dataProvider = "arrays", expectedExceptions = { IndexException.class })
    public <T> void replaceFailsIfReplacementKeyAlreadyExist(Array<T> array) {
        final Index<T> index = Index.of(array.copy(0, array.length() / 2));
        index.replace(array.getValue(0), array.getValue(1));
    }


    @Test(dataProvider = "arrays")
    public <T> void indexIgnoresDuplicates(Array<T> array) {
        Assert.assertFalse(Index.of(array).add(array.getValue(0)), "Index ignores duplicates");
    }


    @Test(dataProvider = "arrays")
    public <T> void filterWithArray(Array<T> array) {
        final Index<T> index = Index.of(array);
        final Array<T> keys = array.copy(IntStream.range(0, array.length()).filter(i -> i % 2 == 0).toArray());
        final Index<T> filter = index.filter(keys);
        Assert.assertTrue(filter.isFilter(), "The filter indicates it is a filter");
        Assert.assertEquals(keys.length(), filter.size(), "Filter has expected size");
    }


    @Test(dataProvider = "arrays")
    public <T> void filterOfFilterTest(Array<T> array) {
        final Index<T> index = Index.of(array);
        final Index<T> filter1 = index.filter(array.copy(10, 100));
        final Index<T> filter2 = filter1.filter(array.copy(20, 30));
        Assert.assertEquals(filter1.size(), 90, "The second filter has expected size");
        Assert.assertEquals(filter2.size(), 10, "The second filter has expected size");
        Assert.assertFalse(filter1.containsAll(array));
        Assert.assertFalse(filter2.containsAll(array));
        for (int i=0; i<filter2.size(); ++i) {
            final T actual = filter2.getKey(i);
            final T expected = array.getValue(i + 20);
            Assert.assertEquals(actual, expected, "The value matches for ordinal " + i);
            Assert.assertEquals(filter1.getIndexForKey(actual), index.getIndexForKey(actual), "Canonical index matches for filter2");
            Assert.assertEquals(filter2.getIndexForKey(actual), index.getIndexForKey(actual), "Canonical index matches for filter2");
        }
    }


    @Test(dataProvider = "arrays")
    public <T> void filterWithPredicate(Array<T> array) {
        final Index<T> index = Index.of(array);
        final Array<T> keys = array.copy(IntStream.range(0, array.length()).filter(i -> i % 2 == 0).toArray());
        final Index<T> filter = index.filter(Predicates.in(keys));
        Assert.assertTrue(filter.isFilter(), "The filter indicates it is a filter");
        Assert.assertEquals(keys.length(), filter.size(), "Filter has expected size");
    }


    @Test(dataProvider = "arrays", expectedExceptions = { IndexException.class })
    public <T> void filterIsImmutable1(Array<T> array) {
        Index.of(array).filter(array.copy(0, 5)).add(array.getValue(0));
    }


    @Test(dataProvider = "arrays", expectedExceptions = { IndexException.class })
    public <T> void filterIsImmutable2(Array<T> array) {
        Index.of(array).filter(array.copy(0, 10)).addAll(array.copy(0,3), true);
    }


    @Test(dataProvider = "arrays", expectedExceptions = { IndexException.class })
    public <T> void exceptionWhenRequestingIndexForNonExistingKey(Array<T> array) {
        Index.of(array).filter(array.copy(0, 10)).getIndexForKey(array.getValue(20));
    }


    @Test(dataProvider = "arrays", expectedExceptions = { IndexException.class })
    public <T> void exceptionWhenRequestingOrdinalForNonExistingKey(Array<T> array) {
        Index.of(array).filter(array.copy(0, 10)).getOrdinalForKey(array.getValue(20));
    }


    @Test(dataProvider = "arrays")
    public <T> void intersectKeys(Array<T> array) {
        final Index<T> index = Index.of(array.copy());
        final Array<T> expected = array.copy(100, 500);
        final Array<T> actual = index.intersect(expected);
        Assert.assertEquals(actual.length(), expected.length(), "Lengths match");
        Assert.assertEquals(actual, expected, "Intersection keys match expected");
    }


    /**
     * Asserts that the two indexes are the same
     * @param index1    the first index
     * @param index2    the second index
     * @param <T>       the index element type
     */
    private <T> void assertEquals(Index<T> index1, Index<T> index2) {
        Assert.assertEquals(index1.size(), index2.size(), "The index sizes match");
        Assert.assertEquals(index1.type(), index2.type(), "The index types match");
        for (int i=0; i<index1.size(); ++i) {
            final T key1 = index1.getKey(i);
            final T key2 = index2.getKey(i);
            Assert.assertEquals(key1, key2, "The keys match at ordinal " + i);
            Assert.assertEquals(index1.getOrdinalForKey(key1), index2.getOrdinalForKey(key2), "The ordinals match for key: " + key1);
            Assert.assertEquals(index1.getIndexForKey(key1), index2.getIndexForKey(key2), "The indexes match for key: " + key1);
            Assert.assertEquals(index1.getOrdinalForKey(key1), index2.getOrdinalForKey(key2), "The ordinals match for key: " + key1);
        }
    }

    /**
     * Returns a Morpheus array from the iterator
     * @param iterator      the iterator reference
     * @param <T>           the element type
     * @return              the Morpheus array
     */
    private <T> Array<T> toArray(Iterator<T> iterator) {
        final ArrayBuilder<T> builder = ArrayBuilder.of(1000);
        while (iterator.hasNext()) builder.add(iterator.next());
        return builder.toArray();
    }
}
