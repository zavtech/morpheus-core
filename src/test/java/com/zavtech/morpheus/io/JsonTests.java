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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;

import com.zavtech.morpheus.TestSuite;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.reference.DataFrameAsserts;
import com.zavtech.morpheus.reference.TestDataFrames;
import com.zavtech.morpheus.util.text.Formats;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests for reading / writing DataFrame JSON files
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class JsonTests {

    @DataProvider(name="ticker")
    public Object[][] getTickers() {
        return new Object[][] { {"blk"}, {"csco"}, {"spy"}, {"yhoo"} };
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
    public <T> void testReadWriteJsonWithLocalDateRowAxis(Class<T> rowType) throws Exception {
        final File file = TestSuite.getOutputFile("JsonTests", "DataFrame-" + rowType.getSimpleName() + ".json");
        final DataFrame<T,String> frame = TestDataFrames.createMixedRandomFrame(rowType, 100);
        frame.out().print();;
        frame.out().writeJson(file);
        frame.out().print();
        readAndValidate(frame, file);
    }


    /**
     * Loads the DataFrame from the file and compares it to original
     * @param original  the original frame
     * @param file      the file to read from
     */
    public <T> void readAndValidate(DataFrame<T,String> original, File file) {
        final DataFrame<T,String> result = DataFrame.read().json(options -> {
            options.setResource(file.getAbsolutePath());
        });
        original.out().print();
        result.out().print();
        DataFrameAsserts.assertEqualsByIndex(original, result);
    }

    @Test(dataProvider="ticker")
    public void testWriteFollowedByRead(String ticker) throws Exception {
        final File file = TestSuite.getOutputFile("JsonTests", "DataFrame-" + ticker + ".json");
        final DataFrame<LocalDate,String> frame1 = TestDataFrames.getQuotes(ticker);
        frame1.out().writeJson(file, new Formats());
        final DataFrame<LocalDate,String> frame2 = DataFrame.read().json(options -> options.setResource(file.getAbsolutePath()));
        DataFrameAsserts.assertEqualsByIndex(frame1, frame2);
    }


    @Test()
    public void testWindowsTaskListCsvRead1() throws Exception {
        final File jsonFile = TestSuite.getOutputFile("JsonTests", "DataFrame-TaskList.json");
        final DataFrame<Integer,String> frame1 = DataFrame.read().csv(options -> {
            options.setResource("/csv/tasks.csv");
            options.setHeader(true);
            options.setRowKeyParser(Integer.class, values -> Integer.parseInt(values[1]));
            options.getFormats().setNullValues("null");
        });
        frame1.out().writeJson(jsonFile);
        final DataFrame<Integer,String> frame2 = DataFrame.read().json(options -> {
            options.setResource(jsonFile.getAbsolutePath());
            options.getFormats().setNullValues("null");
        });
        DataFrameAsserts.assertEqualsByIndex(frame1, frame2);
    }



}
