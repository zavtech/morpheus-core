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
package com.zavtech.morpheus.reference;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameAsserts;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.range.Range;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for DataFrame filter operations
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class FilterTests {


    @Test()
    public void testRowFiltering() {
        final DataFrame<LocalDate,String> frame = createTestFrame1();
        final DataFrame<LocalDate,String> filter = frame.rows().select(row -> row.key().getDayOfWeek() == DayOfWeek.MONDAY);
        Assert.assertTrue(filter.rowCount() > 0, "There is at least one row");
        Assert.assertTrue(filter.rowCount() < frame.rowCount(), "The filter has fewer rows");
        Assert.assertEquals(filter.colCount(), frame.colCount(), "The column count matches");
        filter.rows().keys().forEach(k -> Assert.assertEquals(k.getDayOfWeek(), DayOfWeek.MONDAY, "Key is on Monday"));
        filter.forEachValue(v -> {
            final double actual  = v.getDouble();
            final double expected = frame.data().getDouble(v.rowKey(), v.colKey());
            Assert.assertEquals(actual, expected, "Value matches at " + v);
        });
    }


    @Test()
    public void testColumnFiltering() {
        final DataFrame<String,LocalDate> frame = createTestFrame2();
        final DataFrame<String,LocalDate> filter = frame.cols().select(c -> c.key().getDayOfWeek() == DayOfWeek.MONDAY);
        Assert.assertTrue(filter.colCount() > 0, "There is at least one column");
        Assert.assertTrue(filter.colCount() < frame.colCount(), "The filter has fewer columns");
        Assert.assertEquals(filter.rowCount(), frame.rowCount(), "The row count matches");
        filter.cols().keys().forEach(k -> Assert.assertEquals(k.getDayOfWeek(), DayOfWeek.MONDAY, "Key is on Monday"));
        filter.forEachValue(v -> {
            final double actual = v.getDouble();
            final double expected = frame.data().getDouble(v.rowKey(), v.colKey());
            Assert.assertEquals(actual, expected, "Value matches at " + v);
        });
    }


    @Test()
    public void testRowIndexOfWithFilter() {
        final DataFrame<LocalDate,String> frame = createTestFrame1();
        final DataFrame<LocalDate,String> filter = frame.rows().select(row -> row.key().getDayOfWeek() == DayOfWeek.MONDAY);
        final List<LocalDate> dates = filter.rows().keys().collect(Collectors.toList());
        for (int i=0; i<dates.size(); ++i) {
            final LocalDate date = dates.get(i);
            final int rowIndex = filter.rows().ordinalOf(date, true);
            Assert.assertEquals(i, rowIndex, "Row index match");
        }
    }


    @Test()
    public void testColumnIndexOfWithFilter() {
        final DataFrame<String,LocalDate> frame = createTestFrame2();
        final DataFrame<String,LocalDate> filter = frame.cols().select(row -> row.key().getDayOfWeek() == DayOfWeek.MONDAY);
        final List<LocalDate> dates = filter.cols().keys().collect(Collectors.toList());
        for (int i=0; i<dates.size(); ++i) {
            final LocalDate date = dates.get(i);
            final int colIndex = filter.cols().ordinalOf(date, true);
            Assert.assertEquals(i, colIndex, "Column index match");
        }
    }


    @Test()
    public void testCopyOfRowFilteredFrame() {
        final DataFrame<LocalDate,String> filter = createRowFilteredFrame();
        final DataFrame<LocalDate,String> copy = filter.copy();
        DataFrameAsserts.assertEqualsByIndex(filter, copy);
    }


    @Test()
    public void testCopyOfColumnFilteredFrame() {
        final DataFrame<String,LocalDate> filter = createColumnFilteredFrame();
        final DataFrame<String,LocalDate> copy = filter.copy();
        DataFrameAsserts.assertEqualsByIndex(filter, copy);
    }


    @Test(expectedExceptions={DataFrameException.class})
    public void testRowFilterImmutableInRowDimension1() {
        createRowFilteredFrame().rows().add(LocalDate.now());
    }


    @Test(expectedExceptions={DataFrameException.class})
    public void testRowFilterImmutableInRowDimension2() {
        createRowFilteredFrame().rows().addAll(Arrays.asList(LocalDate.now(), LocalDate.now().plusDays(1)));
    }


    @Test(expectedExceptions={DataFrameException.class})
    public void testRowFilterImmutableInRowDimension3() {
        createRowFilteredFrame().rows().mapKeys(row -> row.key().minusDays(2));
    }


    private DataFrame<LocalDate,String> createRowFilteredFrame() {
        final DataFrame<LocalDate,String> frame = createTestFrame1();
        final DataFrame<LocalDate,String> filter = frame.rows().select(row -> row.key().getDayOfWeek() == DayOfWeek.MONDAY);
        Assert.assertTrue(filter.rowCount() > 0, "There is at least one row");
        return filter;
    }


    private DataFrame<String,LocalDate> createColumnFilteredFrame() {
        final DataFrame<String,LocalDate> frame = createTestFrame2();
        final DataFrame<String,LocalDate> filter = frame.cols().select(row -> row.key().getDayOfWeek() == DayOfWeek.MONDAY);
        Assert.assertTrue(filter.colCount() > 0, "There is at least column row");
        return filter;
    }


    /**
     * Returns a newly created test frame
     * @return          the newly created frame
     */
    private DataFrame<LocalDate,String> createTestFrame1() {
        final LocalDate start = LocalDate.now().minusYears(5);
        final Range<String> columns = Range.of(0, 10).map(i -> "Column-" + i);
        final Range<LocalDate> dates = Range.of(0, 1000).map(start::plusDays);
        return DataFrame.ofDoubles(dates, columns).applyDoubles(v -> 100 * Math.random());
    }

    /**
     * Returns a newly created test frame
     * @return          the newly created frame
     */
    private DataFrame<String,LocalDate> createTestFrame2() {
        final LocalDate start = LocalDate.now().minusYears(5);
        final Range<String> rows = Range.of(0,10).map(i -> "Column-" + i);
        final Range<LocalDate> dates = Range.of(0, 1000).map(start::plusDays);
        return DataFrame.ofDoubles(rows, dates).applyDoubles(v -> 100 * Math.random());
    }

}
