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

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.range.Range;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests for the various factory methods for creating Indexes.
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class IndexCreateTests {

    /**
     * Constructor
     */
    public IndexCreateTests() {
        super();
    }

    @DataProvider(name="ranges")
    public Object[][] ranges() {
        return new Object[][] {
                { Range.of(2000, 5000) },
                { Range.of(2000L, 5000L) },
                { Range.of(2000d, 5000d) },
                { Range.of(1000, 3000).map(i -> "String-" + i) },
                { Range.of(LocalDate.now(), LocalDate.now().plusDays(2000)) },
                { Range.of(LocalTime.of(0,0), LocalTime.of(12, 0), Duration.ofSeconds(1)) },
                { Range.of(LocalDateTime.now(), LocalDateTime.now().plusDays(2), Duration.ofMinutes(1)) },
                { Range.of(ZonedDateTime.now(), ZonedDateTime.now().plusDays(2), Duration.ofMinutes(1)) }
        };
    }


    @DataProvider(name="arrays")
    public Object[][] arrays() {
        return new IndexBasicTests().arrays();
    }


    @Test()
    public void testEmpty() {
        final Index<Integer> index = Index.empty();
        Assert.assertEquals(index.size(), 0);
        Assert.assertEquals(index.type(), Object.class);
    }


    @Test()
    public void testSingleton() {
        final Index<Integer> index = Index.singleton(5);
        Assert.assertEquals(index.size(), 1);
        Assert.assertEquals(index.type(), Integer.class);
    }

    @Test()
    public void testCreateByClass() {
        Assert.assertEquals(Index.of(Boolean.class, 10).type(), Boolean.class);
        Assert.assertEquals(Index.of(Integer.class, 10).type(), Integer.class);
        Assert.assertEquals(Index.of(Long.class, 10).type(), Long.class);
        Assert.assertEquals(Index.of(Double.class, 10).type(), Double.class);
        Assert.assertEquals(Index.of(String.class, 10).type(), String.class);
        Assert.assertEquals(Index.of(LocalTime.class, 10).type(), LocalTime.class);
        Assert.assertEquals(Index.of(LocalDate.class, 10).type(), LocalDate.class);
        Assert.assertEquals(Index.of(LocalDateTime.class, 10).type(), LocalDateTime.class);
        Assert.assertEquals(Index.of(ZonedDateTime.class, 10).type(), ZonedDateTime.class);
        Assert.assertEquals(Index.of(Year.class, 10).type(), Year.class);
    }


    @SuppressWarnings("unchecked")
    @Test(dataProvider = "arrays")
    public <T> void testCreateByIterable1(Array<T> array) {
        final Index<T> index = Index.of((Iterable)array);
        Assert.assertEquals(array.length(), index.size(), "Size matches array length");
        for (int i=0; i<index.size(); ++i) {
            final T key = index.getKey(i);
            Assert.assertEquals(key, array.getValue(i), "Key matches array value at " + i);
            Assert.assertEquals(index.getIndexForKey(key), i, "Index matches for ordinal " + i);
            Assert.assertEquals(index.getOrdinalForKey(key), i, "Ordinals match");
            Assert.assertEquals(index.getIndexForOrdinal(i), i, "Ordinal and index match");
        }
    }


    @SuppressWarnings("unchecked")
    @Test(dataProvider = "ranges")
    public <T> void testCreateByIterable2(Range<T> range) {
        final Index<T> index = Index.of(range);
        final Array<T> array = range.toArray();
        Assert.assertEquals(array.length(), index.size(), "Size matches range length");
        for (int i=0; i<index.size(); ++i) {
            final T key = index.getKey(i);
            Assert.assertEquals(key, array.getValue(i), "Key matches array value at " + i);
            Assert.assertEquals(index.getIndexForKey(key), i, "Index matches for ordinal " + i);
            Assert.assertEquals(index.getOrdinalForKey(key), i, "Ordinals match");
            Assert.assertEquals(index.getIndexForOrdinal(i), i, "Ordinal and index match");
        }
    }


    @SuppressWarnings("unchecked")
    @Test(dataProvider = "arrays")
    public <T> void testCreateByIterable3(Array<T> array) {
        final Index<T> index1 = Index.of((Iterable)array);
        final Index<T> index2 = Index.of(index1);
        Assert.assertEquals(index2.size(), index1.size(), "Size matches array length");
        for (int i=0; i<index2.size(); ++i) {
            final T key = index2.getKey(i);
            Assert.assertEquals(key, index1.getKey(i), "Key matches array value at " + i);
            Assert.assertEquals(index2.getIndexForKey(key), index1.getIndexForKey(key), "Index matches for ordinal " + i);
            Assert.assertEquals(index2.getOrdinalForKey(key), index1.getOrdinalForKey(key), "Ordinals match");
            Assert.assertEquals(index2.getIndexForOrdinal(i), index1.getIndexForOrdinal(i), "Ordinal and index match");
        }
    }


    @SuppressWarnings("unchecked")
    @Test(dataProvider = "arrays")
    public <T> void testCreateByIterable4(Array<T> array) {
        final List<T> list = array.stream().values().collect(Collectors.toList());
        final Index<T> index1 = Index.of((Iterable)list);
        final Index<T> index2 = Index.of((Collection)list);
        Assert.assertEquals(index1.size(), list.size(), "Size matches array length");
        Assert.assertEquals(index2.size(), list.size(), "Size matches array length");
        for (int i=0; i<index1.size(); ++i) {
            final T key = index1.getKey(i);
            Assert.assertEquals(key, list.get(i), "Key matches array value at " + i);
            Assert.assertEquals(index1.getIndexForKey(key), i, "Index matches for ordinal " + i);
            Assert.assertEquals(index1.getOrdinalForKey(key), i, "Ordinals match");
            Assert.assertEquals(index1.getIndexForOrdinal(i), i, "Ordinal and index match");
            Assert.assertEquals(index2.getIndexForKey(key), index1.getIndexForKey(key), "Index matches for ordinal " + i);
            Assert.assertEquals(index2.getOrdinalForKey(key), index1.getOrdinalForKey(key), "Ordinals match");
            Assert.assertEquals(index2.getIndexForOrdinal(i), index1.getIndexForOrdinal(i), "Ordinal and index match");
        }
    }


    @Test()
    public void testAdditionalCreateMethods() {
        final Index<Integer> index1 = Index.of(1, 2, 3, 4, 5, 6, 7);
        final Index<Long> index2 = Index.of(5L, 4L, 3L, 2L, 1L, 0L, -1L);
        final Index<String> index3 = Index.of("a", "b", "c", "d", "e", "f", "g");
        final Index<DayOfWeek> index4 = Index.of(DayOfWeek.class, DayOfWeek.values());
        Assert.assertEquals(index1.size(), 7);
        Assert.assertEquals(index2.size(), 7);
        Assert.assertEquals(index3.size(), 7);
        Assert.assertEquals(index4.size(), 7);
        for (int i=0; i<index1.size(); ++i) {
            switch (i) {
                case 0:
                    Assert.assertEquals(index1.getKey(i).intValue(), 1);
                    Assert.assertEquals(index2.getKey(i).longValue(), 5L);
                    Assert.assertEquals(index3.getKey(i), "a");
                    Assert.assertEquals(index4.getKey(i), DayOfWeek.MONDAY);
                    break;
                case 1:
                    Assert.assertEquals(index1.getKey(i).intValue(), 2);
                    Assert.assertEquals(index2.getKey(i).longValue(), 4L);
                    Assert.assertEquals(index3.getKey(i), "b");
                    Assert.assertEquals(index4.getKey(i), DayOfWeek.TUESDAY);
                    break;
                case 2:
                    Assert.assertEquals(index1.getKey(i).intValue(), 3);
                    Assert.assertEquals(index2.getKey(i).longValue(), 3L);
                    Assert.assertEquals(index3.getKey(i), "c");
                    Assert.assertEquals(index4.getKey(i), DayOfWeek.WEDNESDAY);
                    break;
                case 3:
                    Assert.assertEquals(index1.getKey(i).intValue(), 4);
                    Assert.assertEquals(index2.getKey(i).longValue(), 2L);
                    Assert.assertEquals(index3.getKey(i), "d");
                    Assert.assertEquals(index4.getKey(i), DayOfWeek.THURSDAY);
                    break;
                case 4:
                    Assert.assertEquals(index1.getKey(i).intValue(), 5);
                    Assert.assertEquals(index2.getKey(i).longValue(), 1L);
                    Assert.assertEquals(index3.getKey(i), "e");
                    Assert.assertEquals(index4.getKey(i), DayOfWeek.FRIDAY);
                    break;
                case 5:
                    Assert.assertEquals(index1.getKey(i).intValue(), 6);
                    Assert.assertEquals(index2.getKey(i).longValue(), 0L);
                    Assert.assertEquals(index3.getKey(i), "f");
                    Assert.assertEquals(index4.getKey(i), DayOfWeek.SATURDAY);
                    break;
                case 6:
                    Assert.assertEquals(index1.getKey(i).intValue(), 7);
                    Assert.assertEquals(index2.getKey(i).longValue(), -1L);
                    Assert.assertEquals(index3.getKey(i), "g");
                    Assert.assertEquals(index4.getKey(i), DayOfWeek.SUNDAY);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported index: " + i);
            }
        }
    }


}
