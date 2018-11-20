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
package com.zavtech.morpheus.io;

import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;

import com.zavtech.morpheus.frame.DataFrameException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.zavtech.morpheus.TestSuite;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameAsserts;
import com.zavtech.morpheus.reference.TestDataFrames;
import com.zavtech.morpheus.util.Predicates;
import com.zavtech.morpheus.util.text.Formats;
import com.zavtech.morpheus.util.text.parser.Parser;
import com.zavtech.morpheus.util.text.printer.Printer;

/**
 * A unit test of the DataFrame CSV reader
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class CsvTests {

    private File tmpDir = TestSuite.getOutputDir("csv-tests");
    private String[] quoteFields = {"Date", "Open", "High", "Low", "Close", "Volume", "Adj Close"};


    @DataProvider(name="parallel")
    public Object[][] parallel() {
        return new Object[][] {
            { false },
            { true }
        };
    }


    @DataProvider(name="types")
    public Object[][] types() {
        return new Object[][] {
            {String.class},
            {Integer.class},
            {Long.class},
            {LocalDate.class},
            {LocalTime.class},
            {LocalDateTime.class},
            {ZonedDateTime.class}
        };
    }


    @Test(dataProvider="types")
    public <T> void testLocalDateAxis(Class<T> rowType) throws Exception {
        final String tmpDir = System.getProperty("java.io.tmpdir");
        final File file = new File(tmpDir, "DataFrame-" + rowType.getSimpleName() + ".csv");
        final DataFrame<T,String> frame = TestDataFrames.createMixedRandomFrame(rowType, 100);
        System.out.println("Writing to " + file.getAbsolutePath());
        frame.write().csv(options -> {
            options.setFile(file);
            options.setFormats(formats -> {
                formats.setPrinter("LocalDateTimeColumn", Printer.ofLocalDateTime(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                formats.setPrinter("ZonedDateTimeColumn", Printer.ofZonedDateTime(DateTimeFormatter.ISO_ZONED_DATE_TIME));
            });
        });
        frame.out().print();
        readAndValidate(frame, rowType, file);
    }


    /**
     * Loads the DataFrame from the file and compares it to original
     * @param original  the original frame
     * @param file      the file to read from
     */
    private <T> void readAndValidate(DataFrame<T,String> original, Class<T> rowType, File file) {
        final Formats formats = new Formats();
        final Function<String,T> parser = formats.getParserOrFail(rowType);
        final DataFrame<T,String> result = DataFrame.read().csv(options -> {
            options.setResource(file.getAbsolutePath());
            options.setFormats(formats);
            options.setRowKeyParser(rowType, values -> parser.apply(values[0]));
            options.setExcludeColumns("DataFrame");
            options.getFormats().setParser("DoubleColumn", Double.class);
            options.getFormats().setParser("EnumColumn", Month.class);
            options.getFormats().setParser("LongColumn", Long.class);
            options.getFormats().setParser("IntegerColumn", Integer.class);
            options.getFormats().setParser("LocalDateTimeColumn", Parser.ofLocalDateTime(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            options.getFormats().setParser("ZonedDateTimeColumn", Parser.ofZonedDateTime(DateTimeFormatter.ISO_ZONED_DATE_TIME));
        });
        original.out().print();
        result.out().print();
        DataFrameAsserts.assertEqualsByIndex(original, result);
    }


    @Test()
    public void testBasicRead() throws Exception {
        final DataFrame<Integer,String> frame = DataFrame.read().csv(options -> {
            options.setResource("/csv/aapl.csv");
            options.getFormats().setParser("Volume", Long.class);
        });
        assertTrue(frame.rows().count() > 0, "There is at least one row");
        assertTrue(frame.cols().count() == 7, "There are 7 columns");
        assertTrue(frame.cols().keys().allMatch(Predicates.in(quoteFields)), "Contains all expected columns");
        assertEquals(frame.rowCount(), 8503);
        assertEquals(frame.cols().type("Date"), LocalDate.class);
        assertEquals(frame.cols().type("Open"), Double.class);
        assertEquals(frame.cols().type("High"), Double.class);
        assertEquals(frame.cols().type("Low"), Double.class);
        assertEquals(frame.cols().type("Close"), Double.class);
        assertEquals(frame.cols().type("Volume"), Long.class);
        assertEquals(frame.cols().type("Adj Close"), Double.class);
        assertTrue(frame.rows().firstKey().equals(Optional.of(0)));
        assertTrue(frame.rows().lastKey().equals(Optional.of(8502)));

        assertEquals(frame.data().getValue(0, "Date"), LocalDate.of(1980, 12, 12));
        assertEquals(frame.data().getDouble(0, "Open"), 28.74984, 0.00001);
        assertEquals(frame.data().getDouble(0, "High"), 28.87472, 0.00001);
        assertEquals(frame.data().getDouble(0, "Low"), 28.74984, 0.00001);
        assertEquals(frame.data().getDouble(0, "Close"), 28.74984, 0.00001);
        assertEquals(frame.data().getLong(0, "Volume"), 117258400L);
        assertEquals(frame.data().getDouble(0, "Adj Close"), 0.44203, 0.00001);

        assertEquals(frame.data().getValue(7690, "Date"), LocalDate.of(2011, 6, 8));
        assertEquals(frame.data().getDouble(7690, "Open"), 331.77997, 0.00001);
        assertEquals(frame.data().getDouble(7690, "High"), 334.79999, 0.00001);
        assertEquals(frame.data().getDouble(7690, "Low"), 330.64996, 0.00001);
        assertEquals(frame.data().getDouble(7690, "Close"), 332.24002, 0.00001);
        assertEquals(frame.data().getLong(7690, "Volume"), 83430900L);
        assertEquals(frame.data().getDouble(7690, "Adj Close"), 44.76965, 0.00001);

        assertEquals(frame.data().getValue(8502, "Date"), LocalDate.of(2014, 8, 29));
        assertEquals(frame.data().getDouble(8502, "Open"), 102.86, 0.00001);
        assertEquals(frame.data().getDouble(8502, "High"), 102.9, 0.00001);
        assertEquals(frame.data().getDouble(8502, "Low"), 102.2, 0.00001);
        assertEquals(frame.data().getDouble(8502, "Close"), 102.5, 0.00001);
        assertEquals(frame.data().getLong(8502, "Volume"), 44595000L);
        assertEquals(frame.data().getDouble(8502, "Adj Close"), 101.65627, 0.00001);

        for (int i=0; i<frame.rows().count(); ++i) {
            final Integer rowKey = frame.rows().key(i);
            assertEquals(rowKey.intValue(), i, "Row key matches at index " + i);
        }
    }

    @Test(dataProvider = "parallel")
    public void testRowKeyParser(boolean parallel) throws Exception {
        final DataFrame<LocalDate,String> frame = DataFrame.read().csv(options -> {
            options.setResource("/csv/aapl.csv");
            options.setParallel(parallel);
            options.setExcludeColumns("Date");
            options.setRowKeyParser(LocalDate.class, values -> LocalDate.parse(values[0]));
            options.getFormats().copyParser(Long.class, "Volume");
        });
        assertEquals(frame.rowCount(), 8503);
        assertTrue(frame.cols().count() == 6);
        assertTrue(frame.cols().keys().allMatch(Predicates.in("Open", "High", "Low", "Close", "Volume", "Adj Close")));

        assertEquals(frame.cols().type("Open"), Double.class);
        assertEquals(frame.cols().type("High"), Double.class);
        assertEquals(frame.cols().type("Low"), Double.class);
        assertEquals(frame.cols().type("Close"), Double.class);
        assertEquals(frame.cols().type("Volume"), Long.class);
        assertEquals(frame.cols().type("Adj Close"), Double.class);

        assertEquals(frame.rows().key(0), LocalDate.of(1980, 12, 12));
        assertEquals(frame.data().getDouble(LocalDate.of(1980, 12, 12), "Open"), 28.74984, 0.00001);
        assertEquals(frame.data().getDouble(LocalDate.of(1980, 12, 12), "High"), 28.87472, 0.00001);
        assertEquals(frame.data().getDouble(LocalDate.of(1980, 12, 12), "Low"), 28.74984, 0.00001);
        assertEquals(frame.data().getDouble(LocalDate.of(1980, 12, 12), "Close"), 28.74984, 0.00001);
        assertEquals(frame.data().getLong(LocalDate.of(1980, 12, 12), "Volume"), 117258400L);
        assertEquals(frame.data().getDouble(LocalDate.of(1980, 12, 12), "Adj Close"), 0.44203, 0.00001);

        assertEquals(frame.rows().key(7690), LocalDate.of(2011, 6, 8));
        assertEquals(frame.data().getDouble(LocalDate.of(2011, 6, 8), "Open"), 331.77997, 0.00001);
        assertEquals(frame.data().getDouble(LocalDate.of(2011, 6, 8), "High"), 334.79999, 0.00001);
        assertEquals(frame.data().getDouble(LocalDate.of(2011, 6, 8), "Low"), 330.64996, 0.00001);
        assertEquals(frame.data().getDouble(LocalDate.of(2011, 6, 8), "Close"), 332.24002, 0.00001);
        assertEquals(frame.data().getLong(LocalDate.of(2011, 6, 8), "Volume"), 83430900L);
        assertEquals(frame.data().getDouble(LocalDate.of(2011, 6, 8), "Adj Close"), 44.76965, 0.00001);

        assertEquals(frame.rows().key(8502), LocalDate.of(2014, 8, 29));
        assertEquals(frame.data().getDouble(LocalDate.of(2014, 8, 29), "Open"), 102.86, 0.00001);
        assertEquals(frame.data().getDouble(LocalDate.of(2014, 8, 29), "High"), 102.9, 0.00001);
        assertEquals(frame.data().getDouble(LocalDate.of(2014, 8, 29), "Low"), 102.2, 0.00001);
        assertEquals(frame.data().getDouble(LocalDate.of(2014, 8, 29), "Close"), 102.5, 0.00001);
        assertEquals(frame.data().getLong(LocalDate.of(2014, 8, 29), "Volume"), 44595000L);
        assertEquals(frame.data().getDouble(LocalDate.of(2014, 8, 29), "Adj Close"), 101.65627, 0.00001);

        for (int i=0; i<frame.rows().count(); ++i) {
            final LocalDate rowKey = frame.rows().key(i);
            assertEquals(rowKey.getClass(), LocalDate.class, "Row key matches LocalDate type ");
        }
    }


    @Test(dataProvider = "parallel")
    public void testRowPredicate(boolean parallel) throws Exception {
        final DataFrame<Integer,String> frame = DataFrame.read().csv(options -> {
            options.setResource("/csv/aapl.csv");
            options.setParallel(parallel);
            options.setRowPredicate(values -> values[0].startsWith("2012"));
            options.getFormats().copyParser(Long.class, "Volume");
        });
        assertEquals(frame.rows().count(), 250, "There is at least one row");
        assertEquals(frame.cols().count(), 7, "There are 7 columns");
        assertTrue(frame.cols().keys().allMatch(Predicates.in(quoteFields)), "Contains all expected columns");

        assertEquals(frame.cols().type("Date"), LocalDate.class);
        assertEquals(frame.cols().type("Open"), Double.class);
        assertEquals(frame.cols().type("High"), Double.class);
        assertEquals(frame.cols().type("Low"), Double.class);
        assertEquals(frame.cols().type("Close"), Double.class);
        assertEquals(frame.cols().type("Volume"), Long.class);
        assertEquals(frame.cols().type("Adj Close"), Double.class);

        assertEquals(frame.data().getValue(0, "Date"), LocalDate.of(2012, 1, 3));
        assertEquals(frame.data().getDouble(0, "Open"), 409.39996, 0.00001);
        assertEquals(frame.data().getDouble(0, "High"), 412.5, 0.00001);
        assertEquals(frame.data().getDouble(0, "Low"), 409, 0.00001);
        assertEquals(frame.data().getDouble(0, "Close"), 411.22998, 0.00001);
        assertEquals(frame.data().getLong(0, "Volume"), 75555200L);
        assertEquals(frame.data().getDouble(0, "Adj Close"), 55.41362, 0.00001);

        assertEquals(frame.data().getValue(249, "Date"), LocalDate.of(2012, 12, 31));
        assertEquals(frame.data().getDouble(249, "Open"), 510.53003, 0.00001);
        assertEquals(frame.data().getDouble(249, "High"), 535.39996, 0.00001);
        assertEquals(frame.data().getDouble(249, "Low"), 509, 0.00001);
        assertEquals(frame.data().getDouble(249, "Close"), 532.17004, 0.00001);
        assertEquals(frame.data().getLong(249, "Volume"), 164873100L);
        assertEquals(frame.data().getDouble(249, "Adj Close"), 72.34723, 0.00001);

        frame.rows().forEach(row -> assertTrue(row.<LocalDate>getValue("Date").getYear() == 2012));
    }


    @Test(dataProvider = "parallel")
    public void testColumnPredicate(boolean parallel) throws Exception {
        final String[] columns = {"Date", "Close", "Volume"};
        final DataFrame<Integer,String> frame = DataFrame.read().csv(options -> {
            options.setResource("/csv/aapl.csv");
            options.setIncludeColumns(columns);
            options.setParallel(parallel);
            options.getFormats().setParser("Volume", Long.class);
        });
        final DataFrame<Integer,String> expected = DataFrame.read().csv(options -> {
            options.setResource("/csv/aapl.csv");
            options.getFormats().setParser("Volume", Long.class);
        });
        assertEquals(frame.rowCount(), 8503);
        assertEquals(frame.cols().count(), 3);
        assertTrue(frame.cols().keys().allMatch(Predicates.in(columns)));
        assertEquals(frame.cols().type("Date"), LocalDate.class);
        assertEquals(frame.cols().type("Close"), Double.class);
        assertEquals(frame.cols().type("Volume"), Long.class);
        frame.rows().forEach(row -> {
            for (String column : columns) {
                final Object actual = row.getValue(column);
                final Object expect = expected.data().getValue(row.key(), column);
                assertEquals(actual, expect, "The values match for " + row.key() + ", " + column);
            }
        });
    }


    @Test(dataProvider = "parallel")
    public void testRowAndColumnPredicate(boolean parallel) throws Exception {
        final String[] columns = {"Date", "Close", "Volume"};
        final DataFrame<Integer,String> frame = DataFrame.read().csv(options -> {
            options.setResource("/csv/aapl.csv");
            options.setParallel(parallel);
            options.setRowPredicate(values -> values[0].startsWith("2012"));
            options.setColNamePredicate(Predicates.in(columns));
            options.getFormats().copyParser(Long.class, "Volume");
        });
        final DataFrame<LocalDate,String> expected = DataFrame.read().csv(options -> {
            options.setResource("/csv/aapl.csv");
            options.setRowKeyParser(LocalDate.class, values -> LocalDate.parse(values[0]));
            options.getFormats().copyParser(Long.class, "Volume");
        });
        assertEquals(frame.rows().count(), 250);
        assertEquals(frame.cols().count(), 3);
        assertTrue(frame.cols().keys().allMatch(Predicates.in(columns)));
        assertEquals(frame.cols().type("Date"), LocalDate.class);
        assertEquals(frame.cols().type("Close"), Double.class);
        assertEquals(frame.cols().type("Volume"), Long.class);
        frame.rows().forEach(row -> assertTrue(row.<LocalDate>getValue("Date").getYear() == 2012));
        frame.rows().forEach(row -> {
            final LocalDate date = row.getValue("Date");
            assertTrue(date.getYear() == 2012);
            for (String column : Arrays.asList("Close", "Volume")) {
                final Object actual = row.getValue(column);
                final Object expect = expected.data().getValue(date, column);
                assertEquals(actual, expect, "The values match for " + row.key() + ", " + column);
            }
        });
    }


    @Test()
    public void testWriteFollowedByRead() throws Exception {
        final File file = new File(tmpDir, "aapl.csv");
        final DataFrame<LocalDate,String> frame1 = DataFrame.read().csv(options -> {
            options.setResource("/csv/aapl.csv");
            options.setExcludeColumns("Date");
            options.setRowKeyParser(LocalDate.class, values -> LocalDate.parse(values[0]));
            options.getFormats().setParser("Volume", Long.class);
        });
        frame1.write().csv(o -> o.setFile(file));
        final DataFrame<LocalDate,String> frame2 = DataFrame.read().csv(options -> {
            options.setResource(file.getAbsolutePath());
            options.setExcludeColumns("DataFrame");
            options.getFormats().setParser("Volume", Long.class);
            options.setRowKeyParser(LocalDate.class, values -> LocalDate.parse(values[0]));
        });
        DataFrameAsserts.assertEqualsByIndex(frame1, frame2);
    }


    @Test(dataProvider = "parallel")
    public void testCustomParsers(boolean parallel) throws Exception {
        final DataFrame<LocalDate,String> frame = DataFrame.read().csv(options -> {
            options.setResource("/csv/aapl.csv");
            options.setExcludeColumns("Date");
            options.setParallel(parallel);
            options.setRowKeyParser(LocalDate.class, values -> LocalDate.parse(values[0]));
            options.setFormats(formats -> {
                formats.copyParser(Double.class, "Volume");
                formats.copyParser(BigDecimal.class, "Close");
            });
        });
        assertEquals(frame.rowCount(), 8503);
        assertEquals(frame.cols().count(), 6);
        assertTrue(frame.cols().keys().allMatch(Predicates.in("Open", "High", "Low", "Close", "Volume", "Adj Close")));
        assertEquals(frame.cols().type("Open"), Double.class);
        assertEquals(frame.cols().type("High"), Double.class);
        assertEquals(frame.cols().type("Low"), Double.class);
        assertEquals(frame.cols().type("Close"), BigDecimal.class);
        assertEquals(frame.cols().type("Volume"), Double.class);
        assertEquals(frame.cols().type("Adj Close"), Double.class);
    }


    @Test(dataProvider = "parallel")
    public void testWindowsTasks(boolean parallel) throws Exception {
        final String[] columns = {"Image Name", "PID", "Session Name", "Session#", "Mem Usage", "Status", "User Name", "CPU Time", "Window Title"};
        final DataFrame<Integer,String> frame = DataFrame.read().csv(options -> {
            options.setResource("/csv/tasks.csv");
            options.setParallel(parallel);
            options.setRowKeyParser(Integer.class, values -> Integer.parseInt(values[1]));
            options.setColNamePredicate(Predicates.in(columns));
            options.setExcludeColumns("PID");
        });
        assertEquals(frame.rowCount(), 45, "Frame row count is as expected");
        assertEquals(frame.colCount(), columns.length-1, "Frame column count as expected");
        Arrays.stream(columns).filter(c -> !c.equals("PID")).forEach(column -> assertTrue(frame.cols().contains(column)));
        assertTrue(frame.cols().keys().allMatch(Predicates.in(columns)), "Contains all expected columns");
    }

    @Test()
    public void testWindowsTasksColumnInclude() throws Exception {
        final String[] columns1 = {"Image Name", "Session Name", "Session#", "Mem Usage", "Status", "User Name", "CPU Time", "Window Title"};
        final String[] columns2 = {"Image Name", "Mem Usage", "User Name", "CPU Time"};
        final DataFrame<Integer,String> frame1 = DataFrame.read().csv(options -> {
            options.setResource("/csv/tasks.csv");
            options.setRowKeyParser(Integer.class, values -> Integer.parseInt(values[1]));
            options.setColNamePredicate(Predicates.in(columns1));
        });
        final DataFrame<Integer,String> frame2 = DataFrame.read().csv(options -> {
            options.setResource("/csv/tasks.csv");
            options.setRowKeyParser(Integer.class, values -> Integer.parseInt(values[1]));
            options.setColNamePredicate(Predicates.in(columns2));
        });
        assertEquals(frame1.rowCount(), 45, "Frame1 row count is as expected");
        assertEquals(frame2.rowCount(), 45, "Frame2 row count is as expected");
        assertTrue(frame1.colCount() > frame2.colCount(), "First frame has more columns");
        assertEquals(frame1.colCount(), columns1.length, "Frame1 column count as expected");
        assertEquals(frame2.colCount(), columns2.length, "Frame2 column count as expected");
        Arrays.stream(columns1).forEach(column -> assertTrue(frame1.cols().contains(column)));
        Arrays.stream(columns2).forEach(column -> assertTrue(frame2.cols().contains(column)));
    }

    @Test(dataProvider = "parallel")
    public void testCustomCharset(boolean parallel) throws Exception {
        final DataFrame<Integer,String> frame = DataFrame.read().csv(options -> {
            options.setResource("/csv/process.csv");
            options.setParallel(parallel);
            options.setRowKeyParser(Integer.class, values -> Integer.parseInt(values[29]));
            options.setCharset(StandardCharsets.UTF_16);
            options.setExcludeColumns("ProcessId");
            options.getFormats().copyParser(Long.class, "KernelModeTime");
        });
        assertEquals(frame.rowCount(), 43, "Frame row count is as expected");
        assertEquals(frame.colCount(), 45, "Frame column count is as expected");
    }


    @Test(dataProvider = "parallel")
    public void testMultipleColumnPredicates(boolean parallel) {
        final DataFrame<LocalDate,String> frame = DataFrame.read().csv(options -> {
            options.setResource("/csv/aapl.csv");
            options.setExcludeColumnIndexes(0);
            options.setExcludeColumns("Open");
            options.setParallel(parallel);
            options.setRowKeyParser(LocalDate.class, values -> LocalDate.parse(values[0]));
            options.getFormats().copyParser(Long.class, "Volume");
        });

        assertEquals(frame.rowCount(), 8503);
        assertTrue(frame.cols().count() == 5);
        assertTrue(frame.cols().keys().allMatch(Predicates.in("High", "Low", "Close", "Volume", "Adj Close")));

        assertEquals(frame.cols().type("High"), Double.class);
        assertEquals(frame.cols().type("Low"), Double.class);
        assertEquals(frame.cols().type("Close"), Double.class);
        assertEquals(frame.cols().type("Volume"), Long.class);
        assertEquals(frame.cols().type("Adj Close"), Double.class);

        assertEquals(frame.rows().key(0), LocalDate.of(1980, 12, 12));
        assertEquals(frame.data().getDouble(LocalDate.of(1980, 12, 12), "High"), 28.87472, 0.00001);
        assertEquals(frame.data().getDouble(LocalDate.of(1980, 12, 12), "Low"), 28.74984, 0.00001);
        assertEquals(frame.data().getDouble(LocalDate.of(1980, 12, 12), "Close"), 28.74984, 0.00001);
        assertEquals(frame.data().getLong(LocalDate.of(1980, 12, 12), "Volume"), 117258400L);
        assertEquals(frame.data().getDouble(LocalDate.of(1980, 12, 12), "Adj Close"), 0.44203, 0.00001);

        assertEquals(frame.rows().key(7690), LocalDate.of(2011, 6, 8));
        assertEquals(frame.data().getDouble(LocalDate.of(2011, 6, 8), "High"), 334.79999, 0.00001);
        assertEquals(frame.data().getDouble(LocalDate.of(2011, 6, 8), "Low"), 330.64996, 0.00001);
        assertEquals(frame.data().getDouble(LocalDate.of(2011, 6, 8), "Close"), 332.24002, 0.00001);
        assertEquals(frame.data().getLong(LocalDate.of(2011, 6, 8), "Volume"), 83430900L);
        assertEquals(frame.data().getDouble(LocalDate.of(2011, 6, 8), "Adj Close"), 44.76965, 0.00001);

        assertEquals(frame.rows().key(8502), LocalDate.of(2014, 8, 29));
        assertEquals(frame.data().getDouble(LocalDate.of(2014, 8, 29), "High"), 102.9, 0.00001);
        assertEquals(frame.data().getDouble(LocalDate.of(2014, 8, 29), "Low"), 102.2, 0.00001);
        assertEquals(frame.data().getDouble(LocalDate.of(2014, 8, 29), "Close"), 102.5, 0.00001);
        assertEquals(frame.data().getLong(LocalDate.of(2014, 8, 29), "Volume"), 44595000L);
        assertEquals(frame.data().getDouble(LocalDate.of(2014, 8, 29), "Adj Close"), 101.65627, 0.00001);
    }


    @Test(dataProvider = "parallel")
    public void testIncludeColumnIndexes(boolean parallel) {
        final DataFrame<LocalDate,String> frame = DataFrame.read().csv(options -> {
            options.setResource("/csv/aapl.csv");
            options.setParallel(parallel);
            options.setIncludeColumnIndexes(2, 3, 4, 5, 6);
            options.setRowKeyParser(LocalDate.class, values -> LocalDate.parse(values[0]));
            options.getFormats().copyParser(Long.class, "Volume");
        });

        assertEquals(frame.rowCount(), 8503);
        assertTrue(frame.cols().count() == 5);
        assertTrue(frame.cols().keys().allMatch(Predicates.in("High", "Low", "Close", "Volume", "Adj Close")));

        assertEquals(frame.cols().type("High"), Double.class);
        assertEquals(frame.cols().type("Low"), Double.class);
        assertEquals(frame.cols().type("Close"), Double.class);
        assertEquals(frame.cols().type("Volume"), Long.class);
        assertEquals(frame.cols().type("Adj Close"), Double.class);

        assertEquals(frame.rows().key(0), LocalDate.of(1980, 12, 12));
        assertEquals(frame.data().getDouble(LocalDate.of(1980, 12, 12), "High"), 28.87472, 0.00001);
        assertEquals(frame.data().getDouble(LocalDate.of(1980, 12, 12), "Low"), 28.74984, 0.00001);
        assertEquals(frame.data().getDouble(LocalDate.of(1980, 12, 12), "Close"), 28.74984, 0.00001);
        assertEquals(frame.data().getLong(LocalDate.of(1980, 12, 12), "Volume"), 117258400L);
        assertEquals(frame.data().getDouble(LocalDate.of(1980, 12, 12), "Adj Close"), 0.44203, 0.00001);

        assertEquals(frame.rows().key(7690), LocalDate.of(2011, 6, 8));
        assertEquals(frame.data().getDouble(LocalDate.of(2011, 6, 8), "High"), 334.79999, 0.00001);
        assertEquals(frame.data().getDouble(LocalDate.of(2011, 6, 8), "Low"), 330.64996, 0.00001);
        assertEquals(frame.data().getDouble(LocalDate.of(2011, 6, 8), "Close"), 332.24002, 0.00001);
        assertEquals(frame.data().getLong(LocalDate.of(2011, 6, 8), "Volume"), 83430900L);
        assertEquals(frame.data().getDouble(LocalDate.of(2011, 6, 8), "Adj Close"), 44.76965, 0.00001);

        assertEquals(frame.rows().key(8502), LocalDate.of(2014, 8, 29));
        assertEquals(frame.data().getDouble(LocalDate.of(2014, 8, 29), "High"), 102.9, 0.00001);
        assertEquals(frame.data().getDouble(LocalDate.of(2014, 8, 29), "Low"), 102.2, 0.00001);
        assertEquals(frame.data().getDouble(LocalDate.of(2014, 8, 29), "Close"), 102.5, 0.00001);
        assertEquals(frame.data().getLong(LocalDate.of(2014, 8, 29), "Volume"), 44595000L);
        assertEquals(frame.data().getDouble(LocalDate.of(2014, 8, 29), "Adj Close"), 101.65627, 0.00001);
    }

    @Test
    public void testMaxColumns() {
        DataFrame<String, String> frame = DataFrame.read()
                .csv( options -> {
                    options.setResource("/csv/10001_columns.csv");
                    options.setMaxColumns(10_0001);
                    options.setHeader(false);
                });

        assertEquals(frame.rowCount(), 1, "Must be only one row");
        assertEquals(frame.colCount(), 10001, "Must be default columns + 1");
    }

    @Test(expectedExceptions = {DataFrameException.class})
    public void testMaxColumnsError() {
        DataFrame.read().csv(options -> {
            options.setResource("/csv/10001_columns.csv");
            options.setHeader(false);
        });
    }



    private enum QuoteField {
        OPEN, HIGH, LOW, CLOSE, VOLUME, ADJ_CLOSE;

        public String toString() {
            switch (this) {
                case OPEN:      return "Open";
                case HIGH:      return "High";
                case LOW:       return "Low";
                case CLOSE:     return "Close";
                case VOLUME:    return "Volume";
                case ADJ_CLOSE: return "Adj Close";
                default:    throw new IllegalArgumentException("Unexpected type: " + this.name());
            }
        }

        public static QuoteField parse(String s) {
            if (s.equalsIgnoreCase("Open"))             return OPEN;
            else if (s.equalsIgnoreCase("High"))        return HIGH;
            else if (s.equalsIgnoreCase("Low"))         return LOW;
            else if (s.equalsIgnoreCase("Close"))       return CLOSE;
            else if (s.equalsIgnoreCase("Volume"))      return VOLUME;
            else if (s.equalsIgnoreCase("Adj Close"))   return ADJ_CLOSE;
            else {
                throw new IllegalArgumentException("Unsupported field name: " + s);
            }
        }
    }
}


