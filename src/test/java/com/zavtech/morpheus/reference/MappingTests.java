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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZonedDateTime;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameAsserts;
import com.zavtech.morpheus.frame.DataFrameValue;
import com.zavtech.morpheus.range.Range;

/**
 * Unit tests for the various DataFrame mapToXXX functions
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class MappingTests {


    /**
     * Provides array of test frames
     * @return  array of test frames
     */
    @DataProvider(name="frames")
    public Object[][] frames() {
        return new Object[][] {
            { createTestFrame(1000) },
            { createTestFrame(1000).parallel() },
            { createTestFrame(1000).tail(200) },
            { createTestFrame(1000).tail(200).parallel() },
            { createTestFrame(1000).left(8) },
            { createTestFrame(1000).left(8).parallel() }
        };
    }


    /**
     * Returns a DataFrame initialized with random data of various types
     * @param rowCount  the row count
     * @return          the newly created DataFrame
     */
    @SuppressWarnings("unchecked")
    public static DataFrame<LocalDate,String> createTestFrame(int rowCount) {
        final Random random = new Random();
        final LocalDate start = LocalDate.now().minusDays(rowCount);
        final Range<LocalDate> rowKeys = Range.of(0, rowCount).map(start::plusDays);
        return DataFrame.of(rowKeys, String.class, columns -> {
            columns.add("A", Boolean.class).applyBooleans(v -> random.nextBoolean());
            columns.add("B", Integer.class).applyInts(v -> random.nextInt());
            columns.add("C", Long.class).applyLongs(v -> random.nextLong());
            columns.add("D", Double.class).applyDoubles(v -> random.nextDouble());
            columns.add("E", LocalDate.class).applyValues(v -> LocalDate.now().plusDays(v.rowOrdinal()));
            columns.add("F", LocalTime.class).applyValues(v -> LocalDateTime.now().minusSeconds(v.rowOrdinal()).toLocalTime());
            columns.add("G", LocalDateTime.class).applyValues(v -> LocalDateTime.now().plusDays(v.rowOrdinal()));
            columns.add("H", ZonedDateTime.class).applyValues(v -> ZonedDateTime.now().plusDays(v.rowOrdinal()));
            columns.add("I", Month.class).applyValues(v -> LocalDateTime.now().minusDays(v.rowOrdinal()).getMonth());
        });
    }



    @Test(dataProvider = "frames")
    public void testMapToBooleans(DataFrame<LocalDate,String> source) {
        final DataFrame<LocalDate,String> target = source.mapToBooleans(DataFrameValue::isDouble);
        DataFrameAsserts.assertEqualStructure(source, target);
        target.cols().forEach(col -> Assert.assertEquals(col.typeInfo(), Boolean.class));
        source.forEachValue(v -> {
            final Object sourveValue = v.getValue();
            final boolean isDouble = sourveValue instanceof Double;
            final boolean targetValue = target.data().getBoolean(v.rowOrdinal(), v.colOrdinal());
            Assert.assertEquals(targetValue, isDouble, "Expected result for: " + v);
        });
    }


    @Test(dataProvider = "frames")
    public void testMapToInts(DataFrame<LocalDate,String> source) {
        final DataFrame<LocalDate,String> target = source.mapToInts(v -> {
            final int i = v.rowOrdinal();
            final int j = v.colOrdinal();
            return (i + j) * (i + j + 1) + j;
        });
        DataFrameAsserts.assertEqualStructure(source, target);
        target.cols().forEach(col -> Assert.assertEquals(col.typeInfo(), Integer.class));
        source.forEachValue(v -> {
            final int i = v.rowOrdinal();
            final int j = v.colOrdinal();
            final int expected = (i + j) * (i + j + 1) + j;
            final int targetValue = target.data().getInt(v.rowOrdinal(), v.colOrdinal());
            Assert.assertEquals(targetValue, expected, "Expected result for: " + v);
        });
    }


    @Test(dataProvider = "frames")
    public void testMapToLongs(DataFrame<LocalDate,String> source) {
        final DataFrame<LocalDate,String> target = source.mapToLongs(v -> {
            final int i = v.rowOrdinal();
            final int j = v.colOrdinal();
            return (long)((i + j) * (i + j + 1) + j);
        });
        DataFrameAsserts.assertEqualStructure(source, target);
        target.cols().forEach(col -> Assert.assertEquals(col.typeInfo(), Long.class));
        source.forEachValue(v -> {
            final int i = v.rowOrdinal();
            final int j = v.colOrdinal();
            final long expected = (i + j) * (i + j + 1) + j;
            final long targetValue = target.data().getLong(v.rowOrdinal(), v.colOrdinal());
            Assert.assertEquals(targetValue, expected, "Expected result for: " + v);
        });
    }


    @Test(dataProvider = "frames")
    public void testMapToDoubles(DataFrame<LocalDate,String> source) {
        final DataFrame<LocalDate,String> target = source.mapToDoubles(v -> {
            final int i = v.rowOrdinal();
            final int j = v.colOrdinal();
            return (double)((i + j) * (i + j + 1) + j);
        });
        DataFrameAsserts.assertEqualStructure(source, target);
        target.cols().forEach(col -> Assert.assertEquals(col.typeInfo(), Double.class));
        source.forEachValue(v -> {
            final int i = v.rowOrdinal();
            final int j = v.colOrdinal();
            final double expected = (i + j) * (i + j + 1) + j;
            final double targetValue = target.data().getDouble(v.rowOrdinal(), v.colOrdinal());
            Assert.assertEquals(targetValue, expected, "Expected result for: " + v);
        });
    }


    @Test(dataProvider = "frames")
    public void testMapToObjects(DataFrame<LocalDate,String> source) {
        final DataFrame<LocalDate,String> target = source.mapToObjects(String.class, v -> {
            final int i = v.rowOrdinal();
            final int j = v.colOrdinal();
            return String.format("(%s, %s)", i, j);
        });
        DataFrameAsserts.assertEqualStructure(source, target);
        target.cols().forEach(col -> Assert.assertEquals(col.typeInfo(), String.class));
        source.forEachValue(v -> {
            final int i = v.rowOrdinal();
            final int j = v.colOrdinal();
            final String expected = String.format("(%s, %s)", i, j);
            final String targetValue = target.data().getValue(v.rowOrdinal(), v.colOrdinal());
            Assert.assertEquals(targetValue, expected, "Expected result for: " + v);
        });
    }


    @Test(dataProvider = "frames")
    public void testMapColumnToBooleans(DataFrame<LocalDate,String> source) {
        final DataFrame<LocalDate,String> target = source.mapToBooleans("D", v -> v.getDouble() > 0.5d);
        DataFrameAsserts.assertEqualStructure(source, target);
        DataFrameAsserts.assertEqualsByIndex(
            source.cols().select(col -> !col.key().equals("D")),
            target.cols().select(col -> !col.key().equals("D"))
        );
        Assert.assertEquals(target.colAt("D").typeInfo(), Boolean.class);
        source.forEachValue(v -> {
            if (v.colKey().equals("D")) {
                final boolean expected = v.getDouble() > 0.5d;
                final boolean actual = target.data().getBoolean(v.rowOrdinal(), v.colOrdinal());
                Assert.assertEquals(actual, expected);
            } else {
                final Object v1 = v.getValue();
                final Object v2 = target.data().getValue(v.rowOrdinal(), v.colOrdinal());
                Assert.assertEquals(v1, v2, String.format("Values match at coordinates (%s,%s)", v.rowKey(), v.colKey()));
            }
        });
    }


    @Test(dataProvider = "frames")
    public void testMapColumnToInts(DataFrame<LocalDate,String> source) {
        final DataFrame<LocalDate,String> target = source.mapToInts("D", v -> (int)(v.getDouble() * 100));
        DataFrameAsserts.assertEqualStructure(source, target);
        DataFrameAsserts.assertEqualsByIndex(
            source.cols().select(col -> !col.key().equals("D")),
            target.cols().select(col -> !col.key().equals("D"))
        );
        Assert.assertEquals(target.colAt("D").typeInfo(), Integer.class);
        source.forEachValue(v -> {
            if (v.colKey().equals("D")) {
                final int expected = (int)(v.getDouble() * 100);
                final int actual = target.data().getInt(v.rowOrdinal(), v.colOrdinal());
                Assert.assertEquals(actual, expected, String.format("Values match at coordinates (%s,%s)", v.rowKey(), v.colKey()));
            } else {
                final Object v1 = v.getValue();
                final Object v2 = target.data().getValue(v.rowOrdinal(), v.colOrdinal());
                Assert.assertEquals(v1, v2, String.format("Values match at coordinates (%s,%s)", v.rowKey(), v.colKey()));
            }
        });
    }


    @Test(dataProvider = "frames")
    public void testMapColumnToLongs(DataFrame<LocalDate,String> source) {
        final DataFrame<LocalDate,String> target = source.mapToLongs("D", v -> (long)(v.getDouble() * 100));
        DataFrameAsserts.assertEqualStructure(source, target);
        DataFrameAsserts.assertEqualsByIndex(
            source.cols().select(col -> !col.key().equals("D")),
            target.cols().select(col -> !col.key().equals("D"))
        );
        Assert.assertEquals(target.colAt("D").typeInfo(), Long.class);
        source.forEachValue(v -> {
            if (v.colKey().equals("D")) {
                final long expected = (long)(v.getDouble() * 100);
                final long actual = target.data().getLong(v.rowOrdinal(), v.colOrdinal());
                Assert.assertEquals(actual, expected, String.format("Values match at coordinates (%s,%s)", v.rowKey(), v.colKey()));
            } else {
                final Object v1 = v.getValue();
                final Object v2 = target.data().getValue(v.rowOrdinal(), v.colOrdinal());
                Assert.assertEquals(v1, v2, String.format("Values match at coordinates (%s,%s)", v.rowKey(), v.colKey()));
            }
        });
    }


    @Test(dataProvider = "frames")
    public void testMapColumnToDoubles(DataFrame<LocalDate,String> source) {
        final DataFrame<LocalDate,String> target = source.mapToDoubles("E", v -> v.<LocalDate>getValue().getDayOfMonth() * 10d);
        DataFrameAsserts.assertEqualStructure(source, target);
        DataFrameAsserts.assertEqualsByIndex(
            source.cols().select(col -> !col.key().equals("E")),
            target.cols().select(col -> !col.key().equals("E"))
        );
        Assert.assertEquals(target.colAt("E").typeInfo(), Double.class);
        source.forEachValue(v -> {
            if (v.colKey().equals("E")) {
                final double expected = v.<LocalDate>getValue().getDayOfMonth() * 10d;
                final double actual = target.data().getDouble(v.rowOrdinal(), v.colOrdinal());
                Assert.assertEquals(actual, expected, String.format("Values match at coordinates (%s,%s)", v.rowKey(), v.colKey()));
            } else {
                final Object v1 = v.getValue();
                final Object v2 = target.data().getValue(v.rowOrdinal(), v.colOrdinal());
                Assert.assertEquals(v1, v2, String.format("Values match at coordinates (%s,%s)", v.rowKey(), v.colKey()));
            }
        });
    }



}
