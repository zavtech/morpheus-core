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
import java.time.LocalDate;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.zavtech.morpheus.range.Range;

public class IndexFilterTests {

    @Test()
    public void testLocalDateFilter() {
        final LocalDate start = LocalDate.of(2000, 1, 1);
        final LocalDate today = LocalDate.now();
        final Range<LocalDate> range = Range.of(start, today);
        final Index<LocalDate> index = Index.of(range);
        final Index<LocalDate> mondays = index.filter(d -> d.getDayOfWeek() == DayOfWeek.MONDAY);
        Assert.assertTrue(mondays.size() < index.size());
        for (int i=0; i<mondays.size(); ++i) {
            final LocalDate date = mondays.getKey(i);
            Assert.assertEquals(date.getDayOfWeek(), DayOfWeek.MONDAY);
            Assert.assertEquals(mondays.getIndexForKey(date), index.getIndexForKey(date), "Indexes match");
            Assert.assertEquals(mondays.getIndexForKey(date), mondays.getIndexForOrdinal(i));
        }
    }
}
