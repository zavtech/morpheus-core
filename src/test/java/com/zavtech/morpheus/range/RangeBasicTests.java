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

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.Year;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayType;
import com.zavtech.morpheus.util.IO;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for the Range class
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class RangeBasicTests {


    @DataProvider(name="intRanges")
    public Object[][] intRanges() {
        return new Object[][] {
                { 0, 10000000, 1, false },
                { 0, 10000000, 5, false },
                { 10000000, 6, 1, false },
                { 10000000, 6, 6, false },
                { 0, 10000000, 1, true },
                { 0, 10000000, 5, true },
                { 10000000, 6, 1, true },
                { 10000000, 6, 6, true },
        };
    }

    @DataProvider(name="longRanges")
    public Object[][] longRanges() {
        return new Object[][] {
                { 0L, 10000000L, 1L, false },
                { 0L, 10000000L, 5L, false },
                { 10000000L, 6L, 1L, false },
                { 10000000L, 6L, 6L, false },
                { 0L, 10000000L, 1L, true },
                { 0L, 10000000L, 5L, true },
                { 10000000L, 6L, 1L, true },
                { 10000000L, 6L, 6L, true },
        };
    }

    @DataProvider(name="doubleRanges")
    public Object[][] doubleRanges() {
        return new Object[][] {
                /*{ 0d, 10000000d, 1d, false },
                { 0d, 10000000d, 5d, false },
                { 10000000d, 6d, 1d, false },
                { 10000000d, 6d, 6d, false },
                { 0d, 10000000d, 1d, true },
                { 0d, 10000000d, 5d, true },*/
                { 10000000d, 6d, 1d, true },
                { 10000000d, 6d, 6d, true },
        };
    }

    @DataProvider(name="localDateRanges")
    public Object[][] localDateRanges() {
        return new Object[][] {
                { LocalDate.of(1990, 1, 1), LocalDate.of(1990, 12, 31), Period.ofDays(1), false },
                { LocalDate.of(1990, 1, 1), LocalDate.of(1990, 12, 31), Period.ofDays(5), false },
                { LocalDate.of(2014, 12, 1), LocalDate.of(2013, 1, 1), Period.ofDays(3), false },
                { LocalDate.of(2014, 12, 1), LocalDate.of(2014, 1, 1), Period.ofDays(7), false },
                { LocalDate.of(1990, 1, 1), LocalDate.of(1990, 12, 31), Period.ofDays(1), true },
                { LocalDate.of(1990, 1, 1), LocalDate.of(1990, 12, 31), Period.ofDays(5), true },
                { LocalDate.of(2014, 12, 1), LocalDate.of(2013, 1, 1), Period.ofDays(3), true },
                { LocalDate.of(2014, 12, 1), LocalDate.of(2014, 1, 1), Period.ofDays(7), true },
        };
    }

    @DataProvider(name="localTimeRanges")
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

    @DataProvider(name="localDateTimeRanges")
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
                { ZonedDateTime.of(1990, 1, 1, 9, 0, 0, 0, gmt), ZonedDateTime.of(1990, 3, 1, 13, 0, 0, 0, gmt), Duration.ofSeconds(1), false },
                { ZonedDateTime.of(1990, 1, 1, 9, 0, 0, 0, gmt), ZonedDateTime.of(1990, 3, 1, 15, 30, 0, 0, gmt), Duration.ofSeconds(5), false },
                { ZonedDateTime.of(2014, 12, 1, 7 ,25, 0, 0, gmt), ZonedDateTime.of(2014, 9, 1, 9, 15, 0, 0, gmt), Duration.ofSeconds(1), false },
                { ZonedDateTime.of(2014, 12, 1, 6, 30, 0, 0, gmt), ZonedDateTime.of(2014, 5, 1, 10, 45, 0, 0, gmt), Duration.ofSeconds(7), false },
                { ZonedDateTime.of(1990, 1, 1, 9, 0, 0, 0, gmt), ZonedDateTime.of(1990, 6, 1, 13, 0, 0, 0, gmt), Duration.ofSeconds(1), true },
                { ZonedDateTime.of(1990, 1, 1, 9, 0, 0, 0, gmt), ZonedDateTime.of(1990, 5, 1, 15, 30, 0, 0, gmt), Duration.ofSeconds(5), true },
                { ZonedDateTime.of(2014, 12, 1, 7 ,25, 0, 0, gmt), ZonedDateTime.of(2014, 9, 1, 9, 15, 0, 0, gmt), Duration.ofSeconds(1), true },
                { ZonedDateTime.of(2014, 12, 1, 6, 30, 0, 0, gmt), ZonedDateTime.of(2014, 5, 1, 10, 45, 0, 0, gmt), Duration.ofSeconds(7), true },
        };
    }

    @Test(dataProvider = "intRanges")
    public void testRangeOfInts(int start, int end, int step, boolean parallel) {
        final Range<Integer> range = Range.of(start, end, step);
        final Array<Integer> array = range.toArray(parallel);
        final boolean ascend = start < end;
        final int expectedLength = (int)Math.ceil((double)Math.abs(end - start) / (double)step);
        Assert.assertEquals(array.length(), expectedLength);
        Assert.assertEquals(array.typeCode(), ArrayType.INTEGER);
        Assert.assertTrue(!array.style().isSparse());
        Assert.assertEquals(range.start().intValue(), start, "The range start");
        Assert.assertEquals(range.end().intValue(), end, "The range end");
        for (int i=0; i<array.length(); ++i) {
            final int actual = array.getInt(i);
            final int expected = ascend ? start + i * step : start - i * step;
            Assert.assertEquals(actual, expected, "Value matches at " + i);
            Assert.assertTrue(ascend ? actual >= start && actual < end : actual <= start && actual > end, "Value in bounds at " + i);
        }
    }

    @Test(dataProvider = "longRanges")
    public void testRangeOfLongs(long start, long end, long step, boolean parallel) {
        final Range<Long> range = Range.of(start, end, step);
        final Array<Long> array = range.toArray(parallel);
        final boolean ascend = start < end;
        final long expectedLength = (int)Math.ceil((double)Math.abs(end - start) / (double)step);
        Assert.assertEquals(array.length(), expectedLength);
        Assert.assertEquals(array.typeCode(), ArrayType.LONG);
        Assert.assertTrue(!array.style().isSparse());
        Assert.assertEquals(range.start().longValue(), start, "The range start");
        Assert.assertEquals(range.end().longValue(), end, "The range end");
        for (int i=0; i<array.length(); ++i) {
            final long actual = array.getLong(i);
            final long expected = ascend ? start + i * step : start - i * step;
            Assert.assertEquals(actual, expected, "Value matches at " + i);
            Assert.assertTrue(ascend ? actual >= start && actual < end : actual <= start && actual > end, "Value in bounds at " + i);
        }
    }

    @Test(dataProvider = "doubleRanges")
    public void testRangeOfDoubles(double start, double end, double step, boolean parallel) {
        final Range<Double> range = Range.of(start, end, step);
        final Array<Double> array = range.toArray(parallel);
        final boolean ascend = start < end;
        final int expectedLength = (int)Math.ceil(Math.abs(end - start) / step);
        Assert.assertEquals(array.length(), expectedLength);
        Assert.assertEquals(array.typeCode(), ArrayType.DOUBLE);
        Assert.assertTrue(!array.style().isSparse());
        Assert.assertEquals(range.start(), start, "The range start");
        Assert.assertEquals(range.end(), end, "The range end");
        for (int i=0; i<array.length(); ++i) {
            final double actual = array.getDouble(i);
            final double expected = ascend ? start + i * step : start - i * step;
            Assert.assertEquals(actual, expected, "Value matches at " + i);
            Assert.assertTrue(ascend ? actual >= start && actual < end : actual <= start && actual > end, "Value in bounds at " + i);
        }
    }

    @Test(dataProvider = "localDateRanges")
    public void testRangeOfLocalDates(LocalDate start, LocalDate end, Period step, boolean parallel) {
        final Range<LocalDate> range = Range.of(start, end, step);
        final Array<LocalDate> array = range.toArray(parallel);
        final boolean ascend = start.isBefore(end);
        final int expectedLength = (int)Math.ceil(Math.abs((double)ChronoUnit.DAYS.between(start, end)) / (double)step.getDays());
        Assert.assertEquals(array.length(), expectedLength);
        Assert.assertEquals(array.typeCode(), ArrayType.LOCAL_DATE);
        Assert.assertTrue(!array.style().isSparse());
        Assert.assertEquals(range.start(), start, "The range start");
        Assert.assertEquals(range.end(), end, "The range end");
        LocalDate expected = null;
        for (int i=0; i<array.length(); ++i) {
            final LocalDate actual = array.getValue(i);
            expected = expected == null ? start : ascend ? expected.plus(step) : expected.minus(step);
            Assert.assertEquals(actual, expected, "Value matches at " + i);
            Assert.assertTrue(ascend ? actual.compareTo(start) >=0 && actual.isBefore(end) : actual.compareTo(start) <= 0 && actual.isAfter(end), "Value in bounds at " + i);
        }
    }

    @Test(dataProvider = "localTimeRanges")
    public void testRangeOfLocalTimes(LocalTime start, LocalTime end, Duration step, boolean parallel) {
        final Range<LocalTime> range = Range.of(start, end, step);
        final Array<LocalTime> array = range.toArray(parallel);
        final boolean ascend = start.isBefore(end);
        final int expectedLength = (int)Math.ceil(Math.abs((double)ChronoUnit.SECONDS.between(start, end)) / (double)step.getSeconds());
        Assert.assertEquals(array.length(), expectedLength);
        Assert.assertEquals(array.typeCode(), ArrayType.LOCAL_TIME);
        Assert.assertTrue(!array.style().isSparse());
        Assert.assertEquals(range.start(), start, "The range start");
        Assert.assertEquals(range.end(), end, "The range end");
        LocalTime expected = null;
        for (int i=0; i<array.length(); ++i) {
            final LocalTime actual = array.getValue(i);
            expected = expected == null ? start : ascend ? expected.plus(step) : expected.minus(step);
            Assert.assertEquals(actual, expected, "Value matches at " + i);
            Assert.assertTrue(ascend ? actual.compareTo(start) >=0 && actual.isBefore(end) : actual.compareTo(start) <= 0 && actual.isAfter(end), "Value in bounds at " + i);
        }
    }

    @Test(dataProvider = "localDateTimeRanges")
    public void testRangeOfLocalDateTimes(LocalDateTime start, LocalDateTime end, Duration step, boolean parallel) {
        final Range<LocalDateTime> range = Range.of(start, end, step);
        final Array<LocalDateTime> array = range.toArray(parallel);
        final boolean ascend = start.isBefore(end);
        final int expectedLength = (int)Math.ceil(Math.abs((double)ChronoUnit.SECONDS.between(start, end)) / (double)step.getSeconds());
        Assert.assertEquals(array.length(), expectedLength);
        Assert.assertEquals(array.typeCode(), ArrayType.LOCAL_DATETIME);
        Assert.assertTrue(!array.style().isSparse());
        Assert.assertEquals(range.start(), start, "The range start");
        Assert.assertEquals(range.end(), end, "The range end");
        LocalDateTime expected = null;
        for (int i=0; i<array.length(); ++i) {
            final LocalDateTime actual = array.getValue(i);
            expected = expected == null ? start : ascend ? expected.plus(step) : expected.minus(step);
            Assert.assertEquals(actual, expected, "Value matches at " + i);
            Assert.assertTrue(ascend ? actual.compareTo(start) >=0 && actual.isBefore(end) : actual.compareTo(start) <= 0 && actual.isAfter(end), "Value in bounds at " + i);
        }
    }

    @Test(dataProvider = "ZonedDateTimeRanges")
    public void testRangeOfZonedDateTimes(ZonedDateTime start, ZonedDateTime end, Duration step, boolean parallel) {
        final Range<ZonedDateTime> range = Range.of(start, end, step);
        final Array<ZonedDateTime> array = range.toArray(parallel);
        final boolean ascend = start.isBefore(end);
        final int expectedLength = (int)Math.ceil(Math.abs((double)ChronoUnit.SECONDS.between(start, end)) / (double)step.getSeconds());
        Assert.assertEquals(array.length(), expectedLength);
        Assert.assertEquals(array.typeCode(), ArrayType.ZONED_DATETIME);
        Assert.assertTrue(!array.style().isSparse());
        Assert.assertEquals(range.start(), start, "The range start");
        Assert.assertEquals(range.end(), end, "The range end");
        ZonedDateTime expected = null;
        for (int i=0; i<array.length(); ++i) {
            final ZonedDateTime actual = array.getValue(i);
            expected = expected == null ? start : ascend ? expected.plus(step) : expected.minus(step);
            Assert.assertEquals(actual, expected, "Value matches at " + i);
            Assert.assertTrue(ascend ? actual.compareTo(start) >=0 && actual.isBefore(end) : actual.compareTo(start) <= 0 && actual.isAfter(end), "Value in bounds at " + i);
        }
    }

    @Test()
    public void testRangeWithMapping() {
        final Range<Year> range = Range.of(1990, 2014).map(Year::of);
        final Array<Year> array = range.toArray();
        Assert.assertEquals(array.typeCode(), ArrayType.YEAR);
        Assert.assertTrue(!array.style().isSparse());
        Assert.assertEquals(array.length(), 2014-1990);
        Assert.assertEquals(range.start(), Year.of(1990), "The range start");
        Assert.assertEquals(range.end(), Year.of(2014), "The range end");
        for (int i=1990; i<2014; ++i) {
            final Year expected = Year.of(i);
            Assert.assertEquals(array.getValue(i-1990), expected, "Value matches at " + i);
        }
   }

    @Test()
    public void testMapping() {
        Range.of(5, 10).map(i -> i).forEach(i -> IO.println(i));
    }
}
