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

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameColumns;
import com.zavtech.morpheus.frame.DataFrameRows;

/**
 * Unit tests of covariance estimator in both the row and column dimensions of a DataFrame
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class CovarianceTests {

    @DataProvider(name="style")
    public Object[][] style() {
        return new Object[][] { {false}, {true} };
    }

    private DataFrame<Integer,String> loadSourceData() throws IOException {
        return DataFrame.read().csv("/stats-cov/source-data.csv");
    }

    private DataFrame<Integer,Integer> loadExpectedRowCov() throws IOException {
        return DataFrame.read().<Integer>csv(options -> {
            options.setResource("/stats-cov/row-cov.csv");
            options.setExcludeColumns("Index");
            options.setRowKeyParser(Integer.class, values -> Integer.parseInt(values[0]));
        }).cols().mapKeys(k -> Integer.parseInt(k.key()));
    }

    private DataFrame<String,String> loadExpectedColumnCov() throws IOException {
        return DataFrame.read().csv(options -> {
            options.setResource("/stats-cov/col-cov.csv");
            options.setExcludeColumns("Index");
            options.setRowKeyParser(String.class, values -> values[0]);
        });
    }


    @Test(dataProvider="style")
    public void covarianceOfRows(boolean parallel) throws IOException {
        final DataFrame<Integer,String> source = loadSourceData();
        final DataFrameRows<Integer,String> rows = parallel ? source.rows().parallel() : source.rows().sequential();
        final DataFrame<Integer,Integer> covActual = rows.stats().covariance();
        final DataFrame<Integer,Integer> covExpected = loadExpectedRowCov();
        DataFrameAsserts.assertEqualsByIndex(covExpected, covActual);
        covExpected.cols().keys().forEach(key1 -> covExpected.cols().keys().forEach(key2 -> {
            final double expected = covExpected.data().getDouble(key1, key2);
            final double actual = source.rows().stats().covariance(key1, key2);
            Assert.assertEquals(actual, expected, 0.0000001, "Covariance match for " + key1 + ", " + key2);
        }));
    }


    @Test(dataProvider="style")
    public void covarianceOfColumns(boolean parallel) throws IOException {
        final DataFrame<Integer,String> source = loadSourceData();
        final DataFrameColumns<Integer,String> columns = parallel ? source.cols().parallel() : source.cols().sequential();
        final DataFrame<String,String> covActual = columns.stats().covariance();
        final DataFrame<String,String> covExpected = loadExpectedColumnCov();
        DataFrameAsserts.assertEqualsByIndex(covExpected, covActual);
        covExpected.cols().keys().forEach(key1 -> covExpected.cols().keys().forEach(key2 -> {
            final double expected = covExpected.data().getDouble(key1, key2);
            final double actual = source.cols().stats().covariance(key1, key2);
            Assert.assertEquals(actual, expected, 0.0000001, "Covariance match for " + key1 + ", " + key2);
        }));
    }


    @Test(dataProvider = "style")
    public void testCovarianceWithNonNumericColumns(boolean parallel) throws IOException {
        final DataFrame<Integer,String> source = loadSourceData();
        final DataFrame<Integer,String> input = source.cols().add("NonNumeric", String.class, v -> "Value:" + v.rowOrdinal());
        final DataFrameColumns<Integer,String> columns = parallel ? input.cols().parallel() : input.cols().sequential();
        final DataFrame<String,String> covActual = columns.stats().covariance();
        final DataFrame<String,String> covExpected = loadExpectedColumnCov();
        DataFrameAsserts.assertEqualsByIndex(covExpected, covActual);
        covExpected.cols().keys().forEach(key1 -> covExpected.cols().keys().forEach(key2 -> {
            final double expected = covExpected.data().getDouble(key1, key2);
            final double actual = source.cols().stats().covariance(key1, key2);
            Assert.assertEquals(actual, expected, 0.0000001, "Covariance match for " + key1 + ", " + key2);
        }));
    }


}
