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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests for the Range splitting functionality used for parallel processing
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class RangeSplitTests {


    @DataProvider(name="directions")
    public Object[][] directions() {
        return new Object[][] { { true }, { false } };
    }

    @Test(dataProvider = "directions")
    public void testSplitInts(boolean ascending) {
        final int start = 50000;
        final int end = start + 10000000 * (ascending ? 1 : -1);
        final Range<Integer> range = Range.of(start, end, 1);
        final List<Range<Integer>> segments = range.split();
        Assert.assertTrue(segments.size() > 1, "There are multiple segments");
        for (int i=1; i<segments.size(); ++i) {
            final Range<Integer> prior = segments.get(i-1);
            final Range<Integer> next = segments.get(i);
            Assert.assertEquals(prior.end(), next.start(), "Values connect as expected");
            if (i == 1) Assert.assertEquals(prior.start(), range.start(), "First segment start matches range start");
            if (i == segments.size()-1) {
                Assert.assertEquals(next.end(), range.end(), "Last segment end matches range end");
            }
        }
    }


    @Test(dataProvider = "directions")
    public void testSplitLongs(boolean ascending) {
        final long start = 500000;
        final long end = start + 10000000 * (ascending ? 1 : -1);
        final Range<Long> range = Range.of(start, end, 1L);
        final List<Range<Long>> segments = range.split();
        Assert.assertTrue(segments.size() > 1, "There are multiple segments");
        for (int i=1; i<segments.size(); ++i) {
            final Range<Long> prior = segments.get(i-1);
            final Range<Long> next = segments.get(i);
            Assert.assertEquals(prior.end(), next.start(), "Values connect as expected");
            if (i == 1) Assert.assertEquals(prior.start(), range.start(), "First segment start matches range start");
            if (i == segments.size()-1) {
                Assert.assertEquals(next.end(), range.end(), "Last segment end matches range end");
            }
        }
    }

    @Test(dataProvider = "directions")
    public void testSplitDoubles(boolean ascending) {
        final double start = 50000;
        final double end = start + 10000000 * (ascending ? 1 : -1);
        final Range<Double> range = Range.of(start, end, 1d);
        final List<Range<Double>> segments = range.split();
        Assert.assertTrue(segments.size() > 1, "There are multiple segments");
        for (int i=1; i<segments.size(); ++i) {
            final Range<Double> prior = segments.get(i-1);
            final Range<Double> next = segments.get(i);
            Assert.assertEquals(prior.end(), next.start(), "Values connect as expected");
            if (i == 1) Assert.assertEquals(prior.start(), range.start(), "First segment start matches range start");
            if (i == segments.size()-1) {
                Assert.assertEquals(next.end(), range.end(), "Last segment end matches range end");
            }
        }
    }

    @Test(dataProvider = "directions")
    public void testSplitLocalDate(boolean ascending) {
        final Period step = Period.ofDays(1);
        final LocalDate start = LocalDate.of(1990, 1, 1);
        final LocalDate end = ascending ? start.plusDays(10000) : start.minusDays(10000);
        final Range<LocalDate> range = Range.of(start, end, step);
        final List<Range<LocalDate>> segments = range.split(100);
        Assert.assertTrue(segments.size() > 1, "There are multiple segments");
        for (int i=1; i<segments.size(); ++i) {
            final Range<LocalDate> prior = segments.get(i-1);
            final Range<LocalDate> next = segments.get(i);
            Assert.assertEquals(prior.end(), next.start(), "Date connect as expect");
            if (i == 1) Assert.assertEquals(prior.start(), range.start(), "First segment start matches range start");
            if (i == segments.size()-1) {
                Assert.assertEquals(next.end(), range.end(), "Last segment end matches range end");
            }
        }
    }

    @Test(dataProvider = "directions")
    public void testSplitLocalTime(boolean ascending) {
        final Duration step = Duration.ofMillis(1);
        final LocalTime start = ascending ? LocalTime.of(9, 0) : LocalTime.of(17, 0);
        final LocalTime end = ascending ? start.plusHours(8) : start.minusHours(8);
        final Range<LocalTime> range = Range.of(start, end, step);
        final List<Range<LocalTime>> segments = range.split(100);
        Assert.assertTrue(segments.size() > 1, "There are multiple segments");
        for (int i=1; i<segments.size(); ++i) {
            final Range<LocalTime> prior = segments.get(i-1);
            final Range<LocalTime> next = segments.get(i);
            Assert.assertEquals(prior.end(), next.start(), "Date connect as expect");
            if (i == 1) Assert.assertEquals(prior.start(), range.start(), "First segment start matches range start");
            if (i == segments.size()-1) {
                Assert.assertEquals(next.end(), range.end(), "Last segment end matches range end");
            }
        }
    }


    @Test(dataProvider = "directions")
    public void testSplitLocalDateTime(boolean ascending) {
        final Duration step = Duration.ofSeconds(1);
        final LocalDateTime start = LocalDateTime.of(2014, 1, 1, 14, 15, 20, 0);
        final LocalDateTime end = start.plusSeconds(10000000 * (ascending ? 1 : -1));
        final Range<LocalDateTime> range = Range.of(start, end, step);
        final List<Range<LocalDateTime>> segments = range.split(100);
        Assert.assertTrue(segments.size() > 1, "There are multiple segments");
        for (int i=1; i<segments.size(); ++i) {
            final Range<LocalDateTime> prior = segments.get(i-1);
            final Range<LocalDateTime> next = segments.get(i);
            Assert.assertEquals(prior.end(), next.start(), "Date connect as expect");
            if (i == 1) Assert.assertEquals(prior.start(), range.start(), "First segment start matches range start");
            if (i == segments.size()-1) {
                Assert.assertEquals(next.end(), range.end(), "Last segment end matches range end");
            }
        }
    }


    @Test(dataProvider = "directions")
    public void testSplitZonedDateTimes(boolean ascending) {
        final Duration step = Duration.ofSeconds(1);
        final ZonedDateTime start = ZonedDateTime.of(2014, 1, 1, 14, 15, 20, 0, ZoneId.systemDefault());
        final ZonedDateTime end = start.plusSeconds(10000000 * (ascending ? 1 : -1));
        final Range<ZonedDateTime> range = Range.of(start, end, step);
        final List<Range<ZonedDateTime>> segments = range.split();
        Assert.assertTrue(segments.size() > 1, "There are multiple segments");
        for (int i=1; i<segments.size(); ++i) {
            final Range<ZonedDateTime> prior = segments.get(i-1);
            final Range<ZonedDateTime> next = segments.get(i);
            Assert.assertEquals(prior.end(), next.start(), "Date connect as expect");
            if (i == 1) Assert.assertEquals(prior.start(), range.start(), "First segment start matches range start");
            if (i == segments.size()-1) {
                Assert.assertEquals(next.end(), range.end(), "Last segment end matches range end");
            }
        }
    }
}
