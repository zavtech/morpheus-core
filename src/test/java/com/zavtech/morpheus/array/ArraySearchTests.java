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

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.Period;
import java.time.Year;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Currency;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.IntStream;

import com.zavtech.morpheus.range.Range;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests for array search functionaity
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class ArraySearchTests {

    private static List<Currency> currencyList;
    private static Currency[] currencies = Currency.getAvailableCurrencies().stream().toArray(Currency[]::new);
    private static String[] alphabet = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "x", "y", "z"};

    /**
     * Static initializer
     */
    static {
        Arrays.sort(currencies, (c1, c2) -> c1.getCurrencyCode().compareTo(c2.getCurrencyCode()));
        currencyList = Arrays.asList(currencies);
    }


    @DataProvider(name = "types")
    public Object[][] types() {
        return new ArraysBasicTests().types();
    }


    @Test(dataProvider = "types")
    public <T> void testBinarySearch(Class<T> type, ArrayStyle style) {
        final Array<T> array = createArray(type, style).sort(true);
        final T value = array.getValue(array.length() / 2);
        final int index = array.binarySearch(value);
        final T actual = array.getValue(index);
        Assert.assertEquals(actual, value, "Search found match");
    }


    @Test(dataProvider = "types")
    public <T> void testBinarySearchFirstValue(Class<T> type, ArrayStyle style) {
        final Array<T> array = createArray(type, style).sort(true);
        final T value = array.getValue(0);
        final int index = array.binarySearch(value);
        final T actual = array.getValue(index);
        Assert.assertEquals(actual, value, "Search found match");
    }


    @Test(dataProvider = "types")
    public <T> void testBinarySearchLastValue(Class<T> type, ArrayStyle style) {
        final Array<T> array = createArray(type, style).sort(true);
        final T value = array.getValue(array.length()-1);
        final int index = array.binarySearch(value);
        final T actual = array.getValue(index);
        Assert.assertEquals(actual, value, "Search found match");
    }


    @SuppressWarnings("unchecked")
    @Test(dataProvider = "types")
    public <T> void testBinarySearchWithHigherValue(Class<T> type, ArrayStyle style) {
        final Array<T> array = createArray(type, style).sort(true);
        final T lastValue = array.last(v -> true).map(ArrayValue::getValue).get();
        if (lastValue instanceof Integer) {
            final int index = array.binarySearch(0, array.length(), (T)new Integer(((Integer)lastValue) + 10));
            Assert.assertEquals(index, -1 * (array.length() + 1), "Index implies length() + 1");
        } else if (lastValue instanceof Long) {
            final int index = array.binarySearch(0, array.length(), (T)new Long(((Long)lastValue) + 10));
            Assert.assertEquals(index, -1 * (array.length() + 1), "Index implies length() + 1");
        } else if (lastValue instanceof Double) {
            final int index = array.binarySearch(0, array.length(), (T) new Double(((Double) lastValue) + 10));
            Assert.assertEquals(index, -1 * (array.length() + 1), "Index implies length() + 1");
        } else if (lastValue instanceof String) {
            final String stringValue = (String) lastValue;
            final int index = array.binarySearch(0, array.length(), (T) ("x" + stringValue));
            Assert.assertEquals(index, -1 * (array.length() + 1), "Index implies length() + 1");
        } else if (lastValue instanceof Month) {
            final Month higher = Month.values()[11];
            final int index = array.binarySearch(0, array.length(), (T)higher);
            Assert.assertEquals(index, -1 * (array.length() + 1), "Index implies length() + 1");
        } else if (lastValue instanceof Date) {
            final int index = array.binarySearch(0, array.length(), (T)new Date(((Date)lastValue).getTime() + 5000));
            Assert.assertEquals(index, -1 * (array.length() + 1), "Index implies length() + 1");
        } else if (lastValue instanceof LocalDate) {
            final int index = array.binarySearch(0, array.length(), (T)((LocalDate)lastValue).plusDays(1));
            Assert.assertEquals(index, -1 * (array.length() + 1), "Index implies length() + 1");
        } else if (lastValue instanceof LocalTime) {
            final int index = array.binarySearch(0, array.length(), (T)((LocalTime)lastValue).plusMinutes(2));
            Assert.assertEquals(index, -1 * (array.length() + 1), "Index implies length() + 1");
        } else if (lastValue instanceof LocalDateTime) {
            final int index = array.binarySearch(0, array.length(), (T)((LocalDateTime)lastValue).plusMinutes(2));
            Assert.assertEquals(index, -1 * (array.length() + 1), "Index implies length() + 1");
        } else if (lastValue instanceof ZonedDateTime) {
            final int index = array.binarySearch(0, array.length(), (T) ((ZonedDateTime) lastValue).plusMinutes(2));
            Assert.assertEquals(index, -1 * (array.length() + 1), "Index implies length() + 1");
        } else if (lastValue instanceof Currency) {
            final Currency higher = currencyList.get(currencyList.indexOf(lastValue)+1);
            final int index = array.binarySearch(0, array.length(), (T)higher);
            Assert.assertEquals(index, -1 * (array.length() + 1), "Index implies length() + 1");
        } else if (!(lastValue instanceof Boolean)) {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }


    @SuppressWarnings("unchecked")
    @Test(dataProvider = "types")
    public <T> void testBinarySearchWithLowerValue(Class<T> type, ArrayStyle style) {
        final Array<T> array = createArray(type, style).sort(true);
        final T value = array.first(v -> true).map(ArrayValue::getValue).get();
        if (value instanceof Integer) {
            final int index = array.binarySearch(0, array.length(), (T)new Integer(((Integer)value) - 10));
            Assert.assertEquals(index, -1, "Index implies length() + 1");
        } else if (value instanceof Long) {
            final int index = array.binarySearch(0, array.length(), (T)new Long(((Long)value) - 10));
            Assert.assertEquals(index, -1, "Index implies length() + 1");
        } else if (value instanceof Double) {
            final int index = array.binarySearch(0, array.length(), (T) new Double(((Double) value) - 10));
            Assert.assertEquals(index, -1, "Index implies length() + 1");
        } else if (value instanceof String) {
            final int index = array.binarySearch(0, array.length(), (T) "");
            Assert.assertEquals(index, -1, "Index implies length() + 1");
        } else if (value instanceof Month) {
            final Month lower = Month.values()[0];
            final int index = array.binarySearch(0, array.length(), (T)lower);
            Assert.assertEquals(index, -1, "Index implies length() + 1");
        } else if (value instanceof Date) {
            final int index = array.binarySearch(0, array.length(), (T)new Date(((Date)value).getTime() - 5000));
            Assert.assertEquals(index, -1, "Index implies length() + 1");
        } else if (value instanceof LocalDate) {
            final int index = array.binarySearch(0, array.length(), (T)((LocalDate)value).minusDays(1));
            Assert.assertEquals(index, -1, "Index implies length() + 1");
        } else if (value instanceof LocalTime) {
            final int index = array.binarySearch(0, array.length(), (T)((LocalTime)value).minusSeconds(1));
            Assert.assertEquals(index, -1, "Index implies length() + 1");
        } else if (value instanceof LocalDateTime) {
            final int index = array.binarySearch(0, array.length(), (T)((LocalDateTime)value).minusMinutes(2));
            Assert.assertEquals(index, -1, "Index implies length() + 1");
        } else if (value instanceof ZonedDateTime) {
            final int index = array.binarySearch(0, array.length(), (T)((ZonedDateTime)value).minusMinutes(2));
            Assert.assertEquals(index, -1, "Index implies length() + 1");
        } else if (value instanceof Currency) {
            final Currency lower = currencyList.get(0);
            final int index = array.binarySearch(0, array.length(), (T)lower);
            Assert.assertEquals(index, -1, "Index implies length() + 1");
        } else if (!(value instanceof Boolean)) {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }


    @Test(dataProvider = "types")
    @SuppressWarnings("unchecked")
    public <T> void testFindPreviousValue(Class<T> type, ArrayStyle style) {
        final Function<T,T> previousResolver = v -> {
            if (v instanceof Integer) return (T)(new Integer(((Integer)v) - 1));
            if (v instanceof Long) return (T)(new Long(((Long)v) - 1L));
            if (v instanceof Double) return (T)new Double(((Double)v) - 1d);
            if (v instanceof Date) return (T)new Date(((Date)v).getTime() - 5000);
            if (v instanceof LocalDate) return (T)((LocalDate)v).minusDays(1);
            if (v instanceof LocalTime) return (T)((LocalTime)v).minusNanos(1);
            if (v instanceof LocalDateTime) return (T)((LocalDateTime)v).minusSeconds(1);
            if (v instanceof ZonedDateTime) return (T)((ZonedDateTime)v).minusSeconds(1);
            if (v instanceof Currency) return (T)currencies[currencyList.indexOf(v) - 1];
            if (v instanceof Month) return (T)LocalDate.of(2000, ((Month)v).getValue(), 1).minusDays(10).getMonth();
            if (v instanceof String) return (T)alphabet[Arrays.binarySearch(alphabet, v.toString())-1];
            throw new IllegalArgumentException("Type not supported: " + v);
        };
        if (ArrayType.of(type) != ArrayType.BOOLEAN) {
            final Random random = new Random();
            final Array<T> array = createArray(type, style).sort(true);
            Assert.assertTrue(ArraySortTests.isAscending(array, 0, array.length()), "The array is in ascending order");
            for (int i=0; i<5000; ++i) {
                final int index = random.nextInt(array.length());
                if (index > 0) {
                    final T value = array.getValue(index);
                    final T expectedPrevious = array.getValue(index-1);
                    final Optional<T> previousValue = array.previous(value);
                    Assert.assertTrue(previousValue.isPresent(), "A lower value exists for index " + index);
                    Assert.assertEquals(previousValue.get(), expectedPrevious, "Matches expected previous value for index " + index);
                    final T valueAdjusted = previousResolver.apply(value);
                    final Optional<T> previousValueAdj = array.previous(valueAdjusted);
                    Assert.assertTrue(previousValueAdj.isPresent(), "A lower value exists for index " + index);
                    Assert.assertEquals(previousValueAdj.get(), expectedPrevious, "Matches expected previous value for index " + index);
                }
            }
        }
    }


    @Test(dataProvider = "types")
    @SuppressWarnings("unchecked")
    public <T> void testFindNextValue(Class<T> type, ArrayStyle style) {
        final Function<T,T> nextResolver = v -> {
            if (v instanceof Integer) return (T)(new Integer(((Integer)v) + 1));
            if (v instanceof Long) return (T)(new Long(((Long)v) + 1L));
            if (v instanceof Double) return (T)new Double(((Double)v) + 1d);
            if (v instanceof Date) return (T)new Date(((Date)v).getTime() + 5000);
            if (v instanceof LocalDate) return (T)((LocalDate)v).plusDays(1);
            if (v instanceof LocalTime) return (T)((LocalTime)v).plusNanos(1);
            if (v instanceof LocalDateTime) return (T)((LocalDateTime)v).plusSeconds(1);
            if (v instanceof ZonedDateTime) return (T)((ZonedDateTime)v).plusSeconds(1);
            if (v instanceof Currency) return (T)currencyList.get(currencyList.indexOf(v) + 1);
            if (v instanceof Month) return (T)LocalDate.of(2000, ((Month)v).getValue(), 15).plusDays(25).getMonth();
            if (v instanceof String) return (T)alphabet[Arrays.binarySearch(alphabet, v.toString())+1];
            throw new IllegalArgumentException("Type not supported: " + v);
        };
        if (ArrayType.of(type) != ArrayType.BOOLEAN) {
            final Random random = new Random();
            final Array<T> array = createArray(type, style).sort(true);
            Assert.assertTrue(ArraySortTests.isAscending(array, 0, array.length()), "The array is in ascending order");
            for (int i=0; i<5000; ++i) {
                final int index = random.nextInt(array.length());
                if (index < array.length()-1) {
                    final T value = array.getValue(index);
                    final T expectedNextValue = array.getValue(index+1);
                    final Optional<T> nextValue = array.next(value);
                    Assert.assertTrue(nextValue.isPresent(), "A higher value exists for index " + index);
                    Assert.assertEquals(nextValue.get(), expectedNextValue, "Matches expected next value for index " + index);
                    final T valueAdjusted = nextResolver.apply(value);
                    final Optional<T> nextValueAdj = array.next(valueAdjusted);
                    Assert.assertTrue(nextValueAdj.isPresent(), "A lower value exists for index " + index);
                    Assert.assertEquals(nextValueAdj.get(), expectedNextValue, "Matches expected next value for index " + index);
                }
            }
        }
    }


    @SuppressWarnings("unchecked")
    private static <T> Array<T> createArray(Class<T> type, ArrayStyle style) {
        Object result;
        switch (ArrayType.of(type)) {
            case OBJECT:            result = Range.of(0d, 20000d, 2d);  break;
            case BOOLEAN:           result = Range.of(0, 20000, 2).map(i -> i > 5000);  break;
            case INTEGER:           result = Range.of(0, 20000, 2); break;
            case LONG:              result = Range.of(0L, 20000L, 2L);  break;
            case DOUBLE:            result = Range.of(0d, 20000d, 2d);  break;
            case STRING:            result = Array.of(String.class, alphabet).copy(IntStream.range(2, alphabet.length-3).filter(i -> i % 2 == 0).toArray());   break;
            case CURRENCY:          result = Array.of(currencies).copy(IntStream.range(5, currencies.length-5).filter(i -> i % 2 == 0).toArray());      break;
            case ENUM:              result = Array.of(Month.values()).copy(IntStream.range(1, 12).filter(i -> i % 2 == 0).toArray());    break;
            case YEAR:              result = Range.of(1950, 2030).map(Year::of);                                                    break;
            case DATE:              result = Range.of(0, 20000).map(i -> new Date(System.currentTimeMillis() + i * 10000));         break;
            case LOCAL_DATE:        result = Range.of(LocalDate.now(), LocalDate.now().plusDays(10000), Period.ofDays(2));          break;
            case LOCAL_TIME:        result = Range.of(LocalTime.of(1,0), LocalTime.of(23, 0), Duration.ofSeconds(2));               break;
            case LOCAL_DATETIME:    result = Range.of(LocalDateTime.now(), LocalDateTime.now().plusDays(5), Duration.ofMinutes(2)); break;
            case ZONED_DATETIME:    result = Range.of(ZonedDateTime.now(), ZonedDateTime.now().plusDays(5), Duration.ofMinutes(2)); break;
            default:                throw new RuntimeException("Unsupported type: " + type);
        }
        final float loadFactor = style.isSparse() ? 0.8f : 1f;
        if (result instanceof Range) {
            final Array<T> source = ((Range<T>)result).toArray();
            final Array<T> target = style.isMapped() ? Array.map(type, source.length(), source.defaultValue()) : Array.of(type, source.length(), source.defaultValue(), loadFactor);
            target.applyValues(v -> source.getValue(v.index()));
            return target;
        } else if (result != null) {
            final Array<T> source = (Array<T>)result;
            final Array<T> target = style.isMapped() ? Array.map(type, source.length(), source.defaultValue()) : Array.of(type, source.length(), source.defaultValue(), loadFactor);
            target.applyValues(v -> source.getValue(v.index()));
            return target;
        } else {
            throw new RuntimeException("Unsupported result for type: " + type);
        }
    }

}
