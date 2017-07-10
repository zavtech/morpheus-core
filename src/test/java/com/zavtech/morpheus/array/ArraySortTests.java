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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.Currency;
import java.util.Date;
import java.util.Random;

import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.util.Comparators;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests for sorting arrays using various api calls
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class ArraySortTests {

    private int size = 10000;
    private static Currency[] currencies = Currency.getAvailableCurrencies().stream().toArray(Currency[]::new);


    /**
     * Constructor
     */
    public ArraySortTests() {
        super();
    }


    @DataProvider(name = "types")
    public Object[][] types() {
        return new ArraysBasicTests().types();
    }


    @Test(dataProvider = "types", description = "Tests sorting of all elements in the array")
    public <T> void testSortAscending(Class<T> type, ArrayStyle style) {
        final Array<T> array = random(type, size, style);
        array.sort(true);
        Assert.assertTrue(isAscending(array, 0, array.length()), "Array is in ascending order");
    }


    @Test(dataProvider = "types", description = "Tests sorting of all elements in the array")
    public <T> void testSortDescending(Class<T> type, ArrayStyle style) {
        final Array<T> array = random(type, size, style);
        array.sort(false);
        Assert.assertTrue(isDescending(array, 0, array.length()), "Array is in descending order");
    }


    @SuppressWarnings("unchecked")
    @Test(dataProvider = "types", description = "Tests sorting of all elements in the array")
    public <T> void testSortWithComparatorAscending(Class<T> type, ArrayStyle style) {
        final Array<T> array = random(type, size, style);
        final Comparator<T> comparator = Comparators.getDefaultComparator(array.type());
        array.sort(0, array.length(), (v1, v2) -> comparator.compare(v1.getValue(), v2.getValue()));
        Assert.assertTrue(isAscending(array, 0, array.length()), "Array is in ascending order");
    }


    @SuppressWarnings("unchecked")
    @Test(dataProvider = "types", description = "Tests sorting of all elements in the array")
    public <T> void testSortWithComparatorDescending(Class<T> type, ArrayStyle style) {
        final Array<T> array = random(type, size, style);
        final Comparator<T> comparator = Comparators.getDefaultComparator(array.type());
        array.sort(0, array.length(), (v1, v2) -> -1 * comparator.compare(v1.getValue(), v2.getValue()));
        Assert.assertTrue(isDescending(array, 0, array.length()), "Array is in descending order");
    }


    @Test(dataProvider = "types", description = "Tests sorting a subset of elements in the array")
    public <T> void testSortSubsetAscending(Class<T> type, ArrayStyle style) {
        final Array<T> array = random(type, size, style);
        final Array<T> copy = array.copy();
        array.sort(155, 655, true);
        Assert.assertTrue(isAscending(array, 155, 655));
        Range.of(0, 155).forEach(i -> Assert.assertEquals(array.getValue(i), copy.getValue(i), "Values match at " + i));
        Range.of(655, array.length()).forEach(i -> Assert.assertEquals(array.getValue(i), copy.getValue(i), "Values match at " + i));
    }


    @Test(dataProvider = "types", description = "Tests sorting a subset of elements in the array")
    public <T> void testSortSubsetDescending(Class<T> type, ArrayStyle style) {
        final Array<T> array = random(type, size, style);
        final Array<T> copy = array.copy();
        array.sort(185, 721, false);
        Assert.assertTrue(isDescending(array, 185, 721));
        Range.of(0, 185).forEach(i -> Assert.assertEquals(array.getValue(i), copy.getValue(i), "Values match at " + i));
        Range.of(721, array.length()).forEach(i -> Assert.assertEquals(array.getValue(i), copy.getValue(i), "Values match at " + i));
    }


    @SuppressWarnings("unchecked")
    public static <T> Array<T> random(Class<T> type, int size, ArrayStyle style) {
        final Random random = new Random();
        final Array<T> array = style.isMapped() ? Array.map(type, size) : Array.of(type, size, style.isSparse() ? 0.5f : 1f);
        switch (array.typeCode()) {
            case OBJECT:            array.applyDoubles(v -> random.nextDouble());   break;
            case BOOLEAN:           array.applyBooleans(v -> Math.random() > 0.5d);     break;
            case INTEGER:           array.applyInts(v -> random.nextInt()); break;
            case LONG:              array.applyLongs(v -> random.nextLong());   break;
            case DOUBLE:            array.applyDoubles(v -> random.nextDouble());   break;
            case STRING:            array.applyValues(v -> (T)("X" + random.nextDouble())); break;
            case ENUM:              array.applyValues(v -> (T) Month.of(random.nextInt(11) + 1));  break;
            case CURRENCY:          array.applyValues(v -> (T)currencies[random.nextInt(currencies.length)]);  break;
            case DATE:              array.applyValues(v -> (T)new Date(random.nextInt()));  break;
            case LOCAL_DATE:        array.applyValues(v -> (T)LocalDate.now().plusDays((long)random.nextInt(size)));    break;
            case LOCAL_TIME:        array.applyValues(v -> (T)LocalTime.now().plusNanos((long)random.nextInt(size))); break;
            case LOCAL_DATETIME:    array.applyValues(v -> (T)LocalDateTime.now().plusMinutes((long)random.nextInt(size))); break;
            case ZONED_DATETIME:    array.applyValues(v -> (T)ZonedDateTime.now().plusMinutes((long)random.nextInt(size))); break;
            default:                throw new RuntimeException("Unsupported type: " + type);
        }
        return shuffle(array);
    }

    /**
     * Shuffles the contents of the array
     * @param array     the array to shuffle
     */
    public static <T> Array<T> shuffle(Array<T> array) {
        final Random random = new Random();
        for (int i=0; i<array.length(); ++i) {
            array.swap(i, random.nextInt(array.length()));
        }
        return array;
    }


    @SuppressWarnings("unchecked")
    static <T> boolean isAscending(Array<T> array, int start, int end) {
        Assert.assertTrue(array.length() > 1, "The array has elements");
        final Comparator<T> comparator = Comparators.getDefaultComparator(array.type());
        for (int i=start+1; i<end; ++i) {
            final T v1 = array.getValue(i-1);
            final T v2 = array.getValue(i);
            final int result = comparator.compare(v1, v2);
            if (result > 0) {
                System.out.println("Values are not in ascending order at " + i);
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    static <T> boolean isDescending(Array<T> array, int start, int end) {
        Assert.assertTrue(array.length() > 1, "The array has elements");
        final Comparator<T> comparator = Comparators.getDefaultComparator(array.type());
        for (int i=start+1; i<end; ++i) {
            final T v1 = array.getValue(i-1);
            final T v2 = array.getValue(i);
            final int result = comparator.compare(v1, v2);
            if (result < 0) {
                System.out.println("Values are not in descending order at " + i);
                return false;
            }
        }
        return true;
    }


    @Test()
    public void testEnum() {
        final Random random = new Random();
        final Array<Month> months = Array.of(Month.class, 1000).applyValues(v -> Month.values()[random.nextInt(11)]);
        months.sort(true);
        Assert.assertTrue(isAscending(months, 0, months.length()));

    }
}
