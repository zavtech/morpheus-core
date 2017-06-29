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

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameColumn;
import com.zavtech.morpheus.frame.DataFrameRow;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.index.IndexException;
import com.zavtech.morpheus.range.Range;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * A test of the various DataFrameAccess methods
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class DimensionTests {

    private final LocalDate start = LocalDate.of(2007, 1, 3);
    private final LocalDate end = start.plusDays(20);
    private final Index<LocalDate> rowKeys = Range.of(start, end).toIndex(LocalDate.class);
    private final Index<String> colKeys = Index.of("C1", "C2", "C3", "C4", "C5");
    private final DataFrame<LocalDate,String> frame = DataFrame.ofDoubles(rowKeys, colKeys);


    @Test()
    public void testIndexOf() {
        Assert.assertEquals(frame.rows().ordinalOf(rowKeys.getKey(2)), 2);
        Assert.assertEquals(frame.cols().ordinalOf(colKeys.getKey(4)), 4);
    }


    @Test(expectedExceptions = { IndexException.class })
    public void testIndexOfFailsForRow() {
        Assert.assertEquals(frame.rows().ordinalOf(LocalDate.now(), true), -1);
    }


    @Test(expectedExceptions = { IndexException.class })
    public void testIndexOfFailsForColumn() {
        Assert.assertEquals(frame.cols().ordinalOf("C88", true), -1);
    }


    @Test()
    public void testRowKeyMapping() {
        final DataFrame<String,String> result = frame.rows().mapKeys(row -> row.key().toString());
        rowKeys.forEach(date -> colKeys.forEach(column -> {
            final double value1 = frame.data().getDouble(date, column);
            final double value2 = result.data().getDouble(date.toString(), column);
            Assert.assertEquals(value1, value2, "Values match for " + date + " and " + column);
        }));
    }


    @Test()
    public void testColKeyMapping() {
        final DataFrame<LocalDate,String> result = frame.cols().mapKeys(column -> "Column:" + column.key());
        rowKeys.forEach(date -> colKeys.forEach(column -> {
            final double value1 = frame.data().getDouble(date, column);
            final double value2 = result.data().getDouble(date, "Column:" + column);
            Assert.assertEquals(value1, value2, "Values match for " + date + " and " + column);
        }));
    }


    @Test()
    public void testFirstKey() {
        final DataFrame<LocalDate,String> frame1 = DataFrame.of(rowKeys, colKeys, Double.class);
        final DataFrame<LocalDate,String> frame2 = frame1.copy().select(r -> true, c -> true);
        final DataFrame<LocalDate,String> frame3 = frame1.copy().rows().select(k -> true);
        final DataFrame<LocalDate,String> frame4 = frame1.copy().cols().select(k -> true);
        Stream.of(frame1, frame2, frame3, frame4).forEach(frame -> {
            Assert.assertTrue(frame.rows().firstKey().isPresent());
            Assert.assertTrue(frame.cols().firstKey().isPresent());
            Assert.assertEquals(frame.rows().firstKey().get(), rowKeys.getKey(0));
            Assert.assertEquals(frame.cols().firstKey().get(), colKeys.getKey(0));
        });
        final DataFrame<LocalDate,String> emptyFrame = DataFrame.of(LocalDate.class, String.class);
        Assert.assertTrue(!emptyFrame.rows().firstKey().isPresent());
        Assert.assertTrue(!emptyFrame.cols().firstKey().isPresent());
    }


    @Test()
    public void testLastKey() {
        final DataFrame<LocalDate,String> frame1 = DataFrame.of(rowKeys, colKeys, Double.class);
        final DataFrame<LocalDate,String> frame2 = frame1.copy().select(r -> true, c -> true);
        final DataFrame<LocalDate,String> frame3 = frame1.copy().rows().select(k -> true);
        final DataFrame<LocalDate,String> frame4 = frame1.copy().cols().select(k -> true);
        Stream.of(frame1, frame2, frame3, frame4).forEach(frame -> {
            Assert.assertTrue(frame.rows().lastKey().isPresent());
            Assert.assertTrue(frame.cols().lastKey().isPresent());
            Assert.assertEquals(frame.rows().lastKey().get(), rowKeys.getKey(rowKeys.size() - 1));
            Assert.assertEquals(frame.cols().lastKey().get(), colKeys.getKey(colKeys.size() - 1));
        });
        final DataFrame<LocalDate,String> emptyFrame = DataFrame.of(LocalDate.class, String.class);
        Assert.assertTrue(!emptyFrame.rows().lastKey().isPresent());
        Assert.assertTrue(!emptyFrame.cols().lastKey().isPresent());
    }


    @Test()
    public void testFirstRecordAccess() {
        final DataFrame<String,String> frame1 = TestDataFrames.random(double.class, 20, 30);
        final DataFrame<String,String> frame2 = frame1.copy().select(r -> true, c -> true);
        final DataFrame<String,String> frame3 = frame1.copy().rows().select(k -> true);
        final DataFrame<String,String> frame4 = frame1.copy().cols().select(k -> true);
        Stream.of(frame1, frame2, frame3, frame4).forEach(frame -> {
            Assert.assertTrue(frame.rows().first().isPresent());
            Assert.assertTrue(frame.cols().first().isPresent());
            frame.rows().first().get().forEachValue(v -> Assert.assertEquals(v.getDouble(), frame.data().getDouble(v.rowOrdinal(), v.colOrdinal())));
            frame.cols().first().get().forEachValue(v -> Assert.assertEquals(v.getDouble(), frame.data().getDouble(v.rowOrdinal(), v.colOrdinal())));
        });
    }


    @Test()
    public void testLastRecordAccess() {
        final DataFrame<String,String> frame1 = TestDataFrames.random(double.class, 20, 30);
        final DataFrame<String,String> frame2 = frame1.copy().select(r -> true, c -> true);
        final DataFrame<String,String> frame3 = frame1.copy().rows().select(k -> true);
        final DataFrame<String,String> frame4 = frame1.copy().cols().select(k -> true);
        Stream.of(frame1, frame2, frame3, frame4).forEach(frame -> {
            Assert.assertTrue(frame.rows().last().isPresent());
            Assert.assertTrue(frame.cols().last().isPresent());
            frame.rows().last().get().forEachValue(v -> Assert.assertEquals(v.getDouble(), frame.data().getDouble(v.rowOrdinal(), v.colOrdinal())));
            frame.cols().last().get().forEachValue(v -> Assert.assertEquals(v.getDouble(), frame.data().getDouble(v.rowOrdinal(), v.colOrdinal())));
        });
    }


    @Test()
    public void testFindFirstRow() {
        final DataFrame<String,String> frame1 = TestDataFrames.random(double.class, 10, 30);
        final DataFrame<String,String> frame2 = frame1.copy().select(r -> true, c -> true);
        final DataFrame<String,String> frame3 = frame1.copy().rows().select(k -> true);
        final DataFrame<String,String> frame4 = frame1.copy().cols().select(k -> true);
        Stream.of(frame1, frame2, frame3, frame4).forEach(frame -> {
            frame.applyDoubles(v -> Math.random() * 100d);
            frame.rowAt("R7").applyDoubles(v -> 1000d * v.colOrdinal());
            frame.rowAt("R9").applyDoubles(v -> 1000d * v.colOrdinal());
            final Optional<DataFrameRow<String, String>> rowOption = frame.rows().first(row -> row.getDouble(3) == 1000d * 3);
            Assert.assertTrue(rowOption.isPresent(), "A row was found");
            Assert.assertEquals(rowOption.get().key(), "R7", "The row key matches");
        });
    }


    @Test()
    public void testFindFirstColumn() throws IOException {
        final DataFrame<String,String> frame1 = TestDataFrames.random(double.class, 10, 30);
        final DataFrame<String,String> frame2 = frame1.copy().select(r -> true, c -> true);
        final DataFrame<String,String> frame3 = frame1.copy().rows().select(k -> true);
        final DataFrame<String,String> frame4 = frame1.copy().cols().select(k -> true);
        Stream.of(frame1, frame2, frame3, frame4).forEach(frame -> {
            frame.applyDoubles(v -> Math.random() * 100d);
            frame.colAt("C15").applyDoubles(v -> 1000d * v.rowOrdinal());
            frame.colAt("C22").applyDoubles(v -> 1000d * v.rowOrdinal());
            final Optional<DataFrameColumn<String, String>> colOption = frame.cols().first(column -> column.getDouble(3) == 1000d * 3);
            Assert.assertTrue(colOption.isPresent(), "A column was found");
            Assert.assertEquals(colOption.get().key(), "C15", "The column key matches");
        });
    }


    @Test()
    public void testFindLastRow() {
        final DataFrame<String,String> frame1 = TestDataFrames.random(double.class, 10, 30);
        final DataFrame<String,String> frame2 = frame1.select(r -> true, c -> true);
        Stream.of(frame1, frame2).forEach(frame -> {
            frame.applyDoubles(v -> Math.random() * 100d);
            frame.rowAt("R7").applyDoubles(v -> 1000d * v.colOrdinal());
            frame.rowAt("R9").applyDoubles(v -> 1000d * v.colOrdinal());
            final Optional<DataFrameRow<String, String>> rowOption = frame.rows().last(row -> row.getDouble(3) == 1000d * 3);
            Assert.assertTrue(rowOption.isPresent(), "A row was found");
            Assert.assertEquals(rowOption.get().key(), "R9", "The row key matches");
        });
    }


    @Test()
    public void testFindLastColumn() throws IOException {
        final DataFrame<String,String> frame1 = TestDataFrames.random(double.class, 10, 30);
        final DataFrame<String,String> frame2 = frame1.select(r -> true, c -> true);
        Stream.of(frame1, frame2).forEach(frame -> {
            frame.applyDoubles(v -> Math.random() * 100d);
            frame.colAt("C15").applyDoubles(v -> 1000d * v.rowOrdinal());
            frame.colAt("C22").applyDoubles(v -> 1000d * v.rowOrdinal());
            final Optional<DataFrameColumn<String, String>> colOption = frame.cols().last(column -> column.getDouble(3) == 1000d * 3);
            Assert.assertTrue(colOption.isPresent(), "A column was found");
            Assert.assertEquals(colOption.get().key(), "C22", "The column key matches");
        });
    }


    @Test()
    public void testLowerKeyAccess() {
        final Index<Integer> rowKeys = Range.of(0, 50).map(i -> i * 2).toIndex(Integer.class);
        final Index<Integer> colKeys = Range.of(0, 50).map(i -> i * 2).toIndex(Integer.class);
        final DataFrame<Integer,Integer> frame1 = DataFrame.ofDoubles(rowKeys, colKeys);
        final DataFrame<Integer,Integer> frame2 = frame1.select(r -> true, c -> true);
        Stream.of(frame1, frame2).forEach(frame -> {
            frame.applyDoubles(value -> Math.random());
            Assert.assertTrue(frame.rows().lowerKey(25).isPresent());
            Assert.assertTrue(frame.cols().lowerKey(12).isPresent());
            Assert.assertEquals(frame.rows().lowerKey(25).get().intValue(), 24);
            Assert.assertEquals(frame.cols().lowerKey(12).get().intValue(), 10);
            Assert.assertTrue(!frame.rows().lowerKey(-1).isPresent());
            Assert.assertTrue(!frame.cols().lowerKey(0).isPresent());
        });
    }

    @Test()
    public void testHigherKeyAccess() {
        final Range<Integer> rowKeys = Range.of(0, 50).map(i -> i * 2);
        final Range<Integer> colKeys = Range.of(0, 50).map(i -> i * 2);
        final DataFrame<Integer,Integer> frame1 = DataFrame.ofDoubles(rowKeys, colKeys);
        final DataFrame<Integer,Integer> frame2 = frame1.select(r -> true, c -> true);
        Stream.of(frame1, frame2).forEach(frame -> {
            frame.applyDoubles(value -> Math.random());
            Assert.assertTrue(frame.rows().higherKey(72).isPresent());
            Assert.assertTrue(frame.cols().higherKey(8).isPresent());
            Assert.assertEquals(frame.rows().higherKey(72).get().intValue(), 74);
            Assert.assertEquals(frame.cols().higherKey(8).get().intValue(), 10);
            Assert.assertTrue(!frame.rows().higherKey(102).isPresent());
            Assert.assertTrue(!frame.cols().higherKey(49 * 2).isPresent());
        });
    }

    @Test()
    public void testLowerDateAccess() {
        final ZoneId gmt = ZoneId.of("GMT");
        final Index<ZonedDateTime> dates = Index.of(Arrays.asList(
                ZonedDateTime.of(2014, 5, 2, 9, 30, 0, 0, gmt),
                ZonedDateTime.of(2014, 5, 2, 10, 30, 0, 0, gmt),
                ZonedDateTime.of(2014, 5, 2, 11, 30, 0, 0, gmt),
                ZonedDateTime.of(2014, 5, 2, 12, 30, 0, 0, gmt),
                ZonedDateTime.of(2014, 5, 2, 13, 30, 0, 0, gmt),
                ZonedDateTime.of(2014, 5, 2, 14, 30, 0, 0, gmt),
                ZonedDateTime.of(2014, 5, 2, 15, 30, 0, 0, gmt),
                ZonedDateTime.of(2014, 5, 2, 16, 30, 0, 0, gmt)
        ));
        final DataFrame<ZonedDateTime,ZonedDateTime> frame1 = DataFrame.ofDoubles(dates, dates);
        final DataFrame<ZonedDateTime,ZonedDateTime> frame2 = frame1.select(r -> true, c -> true);
        Stream.of(frame1, frame2).forEach(frame -> {
            frame1.applyDoubles(value -> Math.random());
            Assert.assertTrue(frame.rows().lowerKey(dates.getKey(3)).isPresent());
            Assert.assertTrue(frame.cols().lowerKey(dates.getKey(3)).isPresent());
            Assert.assertEquals(frame.rows().lowerKey(dates.getKey(3)).get(), dates.getKey(2));
            Assert.assertEquals(frame.cols().lowerKey(dates.getKey(6)).get(), dates.getKey(5));
            Assert.assertTrue(!frame.rows().lowerKey(dates.getKey(0).minusDays(1)).isPresent());
            Assert.assertTrue(!frame.cols().lowerKey(dates.getKey(0).minusDays(1)).isPresent());
        });
    }

    @Test()
    public void testHigherDateAccess() {
        final ZoneId gmt = ZoneId.of("GMT");
        final Index<ZonedDateTime> dates = Index.of(Arrays.asList(
                ZonedDateTime.of(2014, 5, 2, 9, 30, 0, 0, gmt),
                ZonedDateTime.of(2014, 5, 2, 10, 30, 0, 0, gmt),
                ZonedDateTime.of(2014, 5, 2, 11, 30, 0, 0, gmt),
                ZonedDateTime.of(2014, 5, 2, 12, 30, 0, 0, gmt),
                ZonedDateTime.of(2014, 5, 2, 13, 30, 0, 0, gmt),
                ZonedDateTime.of(2014, 5, 2, 14, 30, 0, 0, gmt),
                ZonedDateTime.of(2014, 5, 2, 15, 30, 0, 0, gmt),
                ZonedDateTime.of(2014, 5, 2, 16, 30, 0, 0, gmt)
        ));
        final DataFrame<ZonedDateTime,ZonedDateTime> frame1 = DataFrame.ofDoubles(dates, dates);
        final DataFrame<ZonedDateTime,ZonedDateTime> frame2 = frame1.select(r -> true, c -> true);
        Stream.of(frame1, frame2).forEach(frame -> {
            frame.applyDoubles(value -> Math.random());
            Assert.assertTrue(frame.rows().higherKey(dates.getKey(3)).isPresent());
            Assert.assertTrue(frame.cols().higherKey(dates.getKey(3)).isPresent());
            Assert.assertEquals(frame.rows().higherKey(dates.getKey(3)).get(), dates.getKey(4));
            Assert.assertEquals(frame.cols().higherKey(dates.getKey(6)).get(), dates.getKey(7));
            Assert.assertTrue(!frame.rows().higherKey(dates.getKey(7).plusDays(1)).isPresent());
            Assert.assertTrue(!frame.cols().higherKey(dates.getKey(7).plusDays(1)).isPresent());
        });
    }
}
