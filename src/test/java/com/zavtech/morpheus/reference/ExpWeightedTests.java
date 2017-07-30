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

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameAsserts;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * A unit test for the Exponential-Weighted-Moving-Window functions on a DataFrame
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class ExpWeightedTests {


    @DataProvider(name="style")
    public Object[][] style() {
        return new Object[][] {{false}, {true}};
    }


    @Test(dataProvider="style")
    public void testExpWeightedMovingAverage(boolean parallel) {
        final DataFrame<LocalDate,String> expected = DataFrame.read().csv(options -> {
            options.setResource("/ewmw/spy-ewma.csv");
            options.setExcludeColumns("Date");
            options.setRowKeyParser(LocalDate.class, values -> LocalDate.parse(values[0]));
        });
        final DataFrame<LocalDate,String> actual = DataFrame.read().csv(options -> {
            options.setResource("/ewmw/spy.csv");
            options.setExcludeColumns("Date");
            options.getFormats().copyParser(Double.class, "Volume");
            options.setRowKeyParser(LocalDate.class, values -> LocalDate.parse(values[0]));
        });
        if (parallel) {
            final DataFrame<LocalDate,String> ewma = actual.cols().parallel().stats().ewma(20);
            DataFrameAsserts.assertEqualsByIndex(ewma, expected);
        } else {
            final DataFrame<LocalDate,String> ewma = actual.cols().sequential().stats().ewma(20);
            DataFrameAsserts.assertEqualsByIndex(ewma, expected);
        }
    }


    @Test()
    public void testExpWeightedMovingStdDev() {

    }


    @Test()
    public void testExpWeightedMovingVariance() {

    }


}
