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
package com.zavtech.morpheus.range;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayType;
import com.zavtech.morpheus.array.ArrayValue;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for the Range class with filters applied
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class RangeFilterTests {

    @DataProvider(name="IntRanges")
    public Object[][] intRanges() {
        return new Object[][] {
                { 0, 100000, 1, false },
                { 0, 100000, 5, false },
                { 100000, 6, 1, false },
                { 100000, 6, 6, false },
                { 0, 100000, 1, true },
                { 0, 100000, 5, true },
                { 100000, 6, 1, true },
                { 100000, 6, 6, true },
        };
    }

    @DataProvider(name="LongRanges")
    public Object[][] longRanges() {
        return new Object[][] {
                { 0L, 100000L, 1L, false },
                { 0L, 100000L, 5L, false },
                { 100000L, 6L, 1L, false },
                { 100000L, 6L, 6L, false },
                { 0L, 100000L, 1L, true },
                { 0L, 100000L, 5L, true },
                { 100000L, 6L, 1L, true },
                { 100000L, 6L, 6L, true },
        };
    }

    @DataProvider(name="DoubleRanges")
    public Object[][] doubleRanges() {
        return new Object[][] {
                { 0d, 100000d, 1d, false },
                { 0d, 100000d, 5d, false },
                { 100000d, 6d, 1d, false },
                { 100000d, 6d, 6d, false },
                { 0d, 100000d, 1d, true },
                { 0d, 100000d, 5d, true },
                { 100000d, 6d, 1d, true },
                { 100000d, 6d, 6d, true },
        };
    }

    @DataProvider(name="LocalDateRanges")
    public Object[][] localDateRanges() {
        return new Object[][] {
                { LocalDate.of(1990, 1, 1), LocalDate.of(1990, 12, 31), Period.ofDays(1), false },
                { LocalDate.of(1990, 1, 1), LocalDate.of(1990, 12, 31), Period.ofDays(5), false },
                { LocalDate.of(2014, 12, 1), LocalDate.of(2013, 1, 1), Period.ofDays(3), false },
                { LocalDate.of(2014, 12, 1), LocalDate.of(2014, 1, 1), Period.ofDays(6), false },
                { LocalDate.of(1990, 1, 1), LocalDate.of(1990, 12, 31), Period.ofDays(1), true },
                { LocalDate.of(1990, 1, 1), LocalDate.of(1990, 12, 31), Period.ofDays(5), true },
                { LocalDate.of(2014, 12, 1), LocalDate.of(2013, 1, 1), Period.ofDays(3), true },
                { LocalDate.of(2014, 12, 1), LocalDate.of(2014, 1, 1), Period.ofDays(6), true },
        };
    }

    @DataProvider(name="LocalTimeRanges")
    public Object[][] localTimeRanges() {
        return new Object[][] {
                { LocalTime.of(9, 0), LocalTime.of(13, 0), Duration.ofSeconds(1), false },
                { LocalTime.of(9, 0), LocalTime.of(13, 0), Duration.ofSeconds(5), false },
                { LocalTime.of(20, 0), LocalTime.of(13, 0), Duration.ofSeconds(1), false },
                { LocalTime.of(20, 0), LocalTime.of(13, 0), Duration.ofSeconds(7), false },
                { LocalTime.of(9, 0), LocalTime.of(13, 0), Duration.ofSeconds(1), true },
                { LocalTime.of(9, 0), LocalTime.of(13, 0), Duration.ofSeconds(5), true },
                { LocalTime.of(20, 0), LocalTime.of(13, 0), Duration.ofSeconds(1), true },
                { LocalTime.of(20, 0), LocalTime.of(13, 0), Duration.ofSeconds(7), true },
        };
    }

    @DataProvider(name="LocalDateTimeRanges")
    public Object[][] localDateTimeRanges() {
        return new Object[][] {
                { LocalDateTime.of(1990, 1, 1, 9, 0), LocalDateTime.of(1990, 12, 31, 13, 0), Duration.ofMinutes(1), false },
                { LocalDateTime.of(1990, 1, 1, 9, 0), LocalDateTime.of(1990, 12, 31, 15, 30), Duration.ofMinutes(5), false },
                { LocalDateTime.of(2014, 12, 1, 7 ,25), LocalDateTime.of(2013, 1, 1, 9, 15), Duration.ofMinutes(1), false },
                { LocalDateTime.of(2014, 12, 1, 6, 30), LocalDateTime.of(2014, 1, 1, 10, 45), Duration.ofMinutes(7), false },
                { LocalDateTime.of(1990, 1, 1, 9, 0), LocalDateTime.of(1990, 12, 31, 13, 0), Duration.ofMinutes(1), true },
                { LocalDateTime.of(1990, 1, 1, 9, 0), LocalDateTime.of(1990, 12, 31, 15, 30), Duration.ofMinutes(5), true },
                { LocalDateTime.of(2014, 12, 1, 7 ,25), LocalDateTime.of(2013, 1, 1, 9, 15), Duration.ofMinutes(1), true },
                { LocalDateTime.of(2014, 12, 1, 6, 30), LocalDateTime.of(2014, 1, 1, 10, 45), Duration.ofMinutes(7), true },
        };
    }

    @DataProvider(name="ZonedDateTimeRanges")
    public Object[][] zonedDateTimeRanges() {
        final ZoneId gmt = ZoneId.of("GMT");
        return new Object[][] {
                { ZonedDateTime.of(1990, 1, 1, 9, 0, 0, 0, gmt), ZonedDateTime.of(1990, 12, 31, 13, 0, 0, 0, gmt), Duration.ofMinutes(1), false },
                { ZonedDateTime.of(1990, 1, 1, 9, 0, 0, 0, gmt), ZonedDateTime.of(1990, 12, 31, 15, 30, 0, 0, gmt), Duration.ofMinutes(5), false },
                { ZonedDateTime.of(2014, 12, 1, 7 ,25, 0, 0, gmt), ZonedDateTime.of(2013, 1, 1, 9, 15, 0, 0, gmt), Duration.ofMinutes(1), false },
                { ZonedDateTime.of(2014, 12, 1, 6, 30, 0, 0, gmt), ZonedDateTime.of(2014, 1, 1, 10, 45, 0, 0, gmt), Duration.ofMinutes(7), false },
                { ZonedDateTime.of(1990, 1, 1, 9, 0, 0, 0, gmt), ZonedDateTime.of(1990, 12, 31, 13, 0, 0, 0, gmt), Duration.ofMinutes(1), true },
                { ZonedDateTime.of(1990, 1, 1, 9, 0, 0, 0, gmt), ZonedDateTime.of(1990, 12, 31, 15, 30, 0, 0, gmt), Duration.ofMinutes(5), true },
                { ZonedDateTime.of(2014, 12, 1, 7 ,25, 0, 0, gmt), ZonedDateTime.of(2013, 1, 1, 9, 15, 0, 0, gmt), Duration.ofMinutes(1), true },
                { ZonedDateTime.of(2014, 12, 1, 6, 30, 0, 0, gmt), ZonedDateTime.of(2014, 1, 1, 10, 45, 0, 0, gmt), Duration.ofMinutes(7), true },
        };
    }


    @Test(dataProvider = "IntRanges")
    public void testRangeOfInts(int start, int end, int step, boolean parallel) {
        final boolean ascend = start < end;
        final Range<Integer> range = Range.of(start, end, step, (int v) -> v < 10);
        final Array<Integer> array = range.toArray(parallel);
        final int first = array.first(v -> true).map(ArrayValue::getValue).get();
        final int last = array.last(v -> true).map(ArrayValue::getValue).get();
        Assert.assertTrue(array.length() > 0, "There are elements in the array");
        Assert.assertEquals(array.typeCode(), ArrayType.INTEGER);
        Assert.assertTrue(!array.style().isSparse());
        Assert.assertEquals(range.start().intValue(), start, "The range start");
        Assert.assertEquals(range.end().intValue(), end, "The range end");
        int index = 0;
        int value = first;
        while (ascend ? value < last : value > last) {
            final int actual = array.getInt(index);
            Assert.assertEquals(actual, value, "Value matches at " + index);
            Assert.assertTrue(ascend ? actual >= start && actual < end : actual <= start && actual > end, "Value in bounds at " + index);
            value = ascend ? value + step : value - step;
            index++;
        }
    }

    @Test(dataProvider = "LongRanges")
    public void testRangeOfLongs(long start, long end, long step, boolean parallel) {
        final boolean ascend = start < end;
        final Range<Long> range = Range.of(start, end, step, (long v) -> v < 10);
        final Array<Long> array = range.toArray(parallel);
        final long first = array.first(v -> true).map(ArrayValue::getValue).get();
        final long last = array.last(v -> true).map(ArrayValue::getValue).get();
        Assert.assertTrue(array.length() > 0, "There are elements in the array");
        Assert.assertEquals(array.typeCode(), ArrayType.LONG);
        Assert.assertTrue(!array.style().isSparse());
        Assert.assertEquals(range.start().intValue(), start, "The range start");
        Assert.assertEquals(range.end().intValue(), end, "The range end");
        int index = 0;
        long value = first;
        while (ascend ? value < last : value > last) {
            final long actual = array.getLong(index);
            Assert.assertEquals(actual, value, "Value matches at " + index);
            Assert.assertTrue(ascend ? actual >= start && actual < end : actual <= start && actual > end, "Value in bounds at " + index);
            value = ascend ? value + step : value - step;
            index++;
        }
    }

    @Test(dataProvider = "DoubleRanges")
    public void testRangeOfDoubles(double start, double end, double step, boolean parallel) {
        final boolean ascend = start < end;
        final Range<Double> range = Range.of(start, end, step, (double v) -> v < 10);
        final Array<Double> array = range.toArray(parallel);
        final double first = array.first(v -> true).map(ArrayValue::getValue).get();
        final double last = array.last(v -> true).map(ArrayValue::getValue).get();
        Assert.assertTrue(array.length() > 0, "There are elements in the array");
        Assert.assertEquals(array.typeCode(), ArrayType.DOUBLE);
        Assert.assertTrue(!array.style().isSparse());
        Assert.assertEquals(range.start(), start, "The range start");
        Assert.assertEquals(range.end(), end, "The range end");
        int index = 0;
        double value = first;
        while (ascend ? value < last : value > last) {
            final double actual = array.getDouble(index);
            Assert.assertEquals(actual, value, "Value matches at " + index);
            Assert.assertTrue(ascend ? actual >= start && actual < end : actual <= start && actual > end, "Value in bounds at " + index);
            value = ascend ? value + step : value - step;
            index++;
        }
    }

    @Test(dataProvider = "LocalDateRanges")
    public void testRangeOfLocalDates(LocalDate start, LocalDate end, Period step, boolean parallel) {
        final boolean ascend = start.isBefore(end);
        final Range<LocalDate> range = Range.of(start, end, step, v -> v.getDayOfWeek() == DayOfWeek.MONDAY);
        final Array<LocalDate> array = range.toArray(parallel);
        final LocalDate first = array.first(v -> true).map(ArrayValue::getValue).get();
        final LocalDate last = array.last(v -> true).map(ArrayValue::getValue).get();
        Assert.assertEquals(array.typeCode(), ArrayType.LOCAL_DATE);
        Assert.assertTrue(!array.style().isSparse());
        Assert.assertEquals(range.start(), start, "The range start");
        Assert.assertEquals(range.end(), end, "The range end");
        int index = 0;
        LocalDate value = first;
        while (ascend ? value.isBefore(last) : value.isAfter(last)) {
            final LocalDate actual = array.getValue(index);
            Assert.assertEquals(actual, value, "Value matches at " + index);
            Assert.assertTrue(ascend ? actual.compareTo(start) >= 0 && actual.isBefore(end) : actual.compareTo(start) <= 0 && actual.isAfter(end), "Value in bounds at " + index);
            value = ascend ? value.plus(step) : value.minus(step);
            while (value.getDayOfWeek() == DayOfWeek.MONDAY) value = ascend ? value.plus(step) : value.minus(step);
            index++;
        }
    }

    @Test(dataProvider = "LocalTimeRanges")
    public void testRangeOfLocalTimes(LocalTime start, LocalTime end, Duration step, boolean parallel) {
        final boolean ascend = start.isBefore(end);
        final Range<LocalTime> range = Range.of(start, end, step, v -> v.getHour() == 6);
        final Array<LocalTime> array = range.toArray(parallel);
        final LocalTime first = array.first(v -> true).map(ArrayValue::getValue).get();
        final LocalTime last = array.last(v -> true).map(ArrayValue::getValue).get();
        Assert.assertEquals(array.typeCode(), ArrayType.LOCAL_TIME);
        Assert.assertTrue(!array.style().isSparse());
        Assert.assertEquals(range.start(), start, "The range start");
        Assert.assertEquals(range.end(), end, "The range end");
        int index = 0;
        LocalTime value = first;
        while (ascend ? value.isBefore(last) : value.isAfter(last)) {
            final LocalTime actual = array.getValue(index);
            Assert.assertEquals(actual, value, "Value matches at " + index);
            Assert.assertTrue(ascend ? actual.compareTo(start) >= 0 && actual.isBefore(end) : actual.compareTo(start) <= 0 && actual.isAfter(end), "Value in bounds at " + index);
            value = ascend ? value.plus(step) : value.minus(step);
            while (value.getHour() == 6) value = ascend ? value.plus(step) : value.minus(step);
            index++;
        }
    }

    @Test(dataProvider = "LocalDateTimeRanges")
    public void testRangeOfLocalDateTimes(LocalDateTime start, LocalDateTime end, Duration step, boolean parallel) {
        final boolean ascend = start.isBefore(end);
        final Range<LocalDateTime> range = Range.of(start, end, step, v -> v.getHour() == 6);
        final Array<LocalDateTime> array = range.toArray(parallel);
        final LocalDateTime first = array.first(v -> true).map(ArrayValue::getValue).get();
        final LocalDateTime last = array.last(v -> true).map(ArrayValue::getValue).get();
        Assert.assertEquals(array.typeCode(), ArrayType.LOCAL_DATETIME);
        Assert.assertTrue(!array.style().isSparse());
        Assert.assertEquals(range.start(), start, "The range start");
        Assert.assertEquals(range.end(), end, "The range end");
        int index = 0;
        LocalDateTime value = first;
        while (ascend ? value.isBefore(last) : value.isAfter(last)) {
            final LocalDateTime actual = array.getValue(index);
            Assert.assertEquals(actual, value, "Value matches at " + index);
            Assert.assertTrue(ascend ? actual.compareTo(start) >= 0 && actual.isBefore(end) : actual.compareTo(start) <= 0 && actual.isAfter(end), "Value in bounds at " + index);
            value = ascend ? value.plus(step) : value.minus(step);
            while (value.getHour() == 6) value = ascend ? value.plus(step) : value.minus(step);
            index++;
        }
    }

    @Test(dataProvider = "ZonedDateTimeRanges")
    public void testRangeOfZonedDateTimes(ZonedDateTime start, ZonedDateTime end, Duration step, boolean parallel) {
        final boolean ascend = start.isBefore(end);
        final Range<ZonedDateTime> range = Range.of(start, end, step, v -> v.getHour() == 6);
        final Array<ZonedDateTime> array = range.toArray(parallel);
        final ZonedDateTime first = array.first(v -> true).map(ArrayValue::getValue).get();
        final ZonedDateTime last = array.last(v -> true).map(ArrayValue::getValue).get();
        Assert.assertEquals(array.typeCode(), ArrayType.ZONED_DATETIME);
        Assert.assertTrue(!array.style().isSparse());
        Assert.assertEquals(range.start(), start, "The range start");
        Assert.assertEquals(range.end(), end, "The range end");
        int index = 0;
        ZonedDateTime value = first;
        while (ascend ? value.isBefore(last) : value.isAfter(last)) {
            final ZonedDateTime actual = array.getValue(index);
            Assert.assertEquals(actual, value, "Value matches at " + index);
            Assert.assertTrue(ascend ? actual.compareTo(start) >= 0 && actual.isBefore(end) : actual.compareTo(start) <= 0 && actual.isAfter(end), "Value in bounds at " + index);
            value = ascend ? value.plus(step) : value.minus(step);
            while (value.getHour() == 6) value = ascend ? value.plus(step) : value.minus(step);
            index++;
        }
    }
}
