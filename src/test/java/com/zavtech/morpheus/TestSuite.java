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
package com.zavtech.morpheus;

import java.io.File;

import com.zavtech.morpheus.array.ArrayBoundsTests;
import com.zavtech.morpheus.array.ArrayBuilderTests;
import com.zavtech.morpheus.array.ArrayFillTests;
import com.zavtech.morpheus.array.ArrayFuncTests;
import com.zavtech.morpheus.array.ArrayIOTests;
import com.zavtech.morpheus.array.ArrayMappedTests;
import com.zavtech.morpheus.array.ArraySearchTests;
import com.zavtech.morpheus.array.ArraySortTests;
import com.zavtech.morpheus.array.ArrayStatsTests;
import com.zavtech.morpheus.array.ArrayUpdateTests;
import com.zavtech.morpheus.array.ArraysBasicTests;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.index.IndexBasicTests;
import com.zavtech.morpheus.index.IndexCreateTests;
import com.zavtech.morpheus.index.IndexSortTests;
import com.zavtech.morpheus.io.DbTests;
import com.zavtech.morpheus.reference.AccessTests;
import com.zavtech.morpheus.reference.AlgebraTests;
import com.zavtech.morpheus.reference.BinarySearchTests;
import com.zavtech.morpheus.reference.BoundsTests;
import com.zavtech.morpheus.reference.ColumnAccessTests;
import com.zavtech.morpheus.reference.ColumnTests;
import com.zavtech.morpheus.reference.CopyTests;
import com.zavtech.morpheus.reference.CorrelationTests;
import com.zavtech.morpheus.reference.CovarianceTests;
import com.zavtech.morpheus.reference.CreateTests;
import com.zavtech.morpheus.reference.DimensionTests;
import com.zavtech.morpheus.reference.EqualsTest;
import com.zavtech.morpheus.reference.EventTests;
import com.zavtech.morpheus.reference.ExpWeightedTests;
import com.zavtech.morpheus.reference.ExportTests;
import com.zavtech.morpheus.reference.FillTests;
import com.zavtech.morpheus.reference.FilterTests;
import com.zavtech.morpheus.reference.GLSTests;
import com.zavtech.morpheus.reference.GroupingTests;
import com.zavtech.morpheus.reference.IteratorTests;
import com.zavtech.morpheus.reference.ApplyTests;
import com.zavtech.morpheus.reference.MappingTests;
import com.zavtech.morpheus.reference.MinMaxTests;
import com.zavtech.morpheus.reference.PCATests;
import com.zavtech.morpheus.reference.QuoteTests;
import com.zavtech.morpheus.reference.RankTests;
import com.zavtech.morpheus.reference.RowAccessTests;
import com.zavtech.morpheus.reference.RowTests;
import com.zavtech.morpheus.reference.OLSTests;
import com.zavtech.morpheus.reference.SelectTests;
import com.zavtech.morpheus.reference.SerializationTests;
import com.zavtech.morpheus.reference.SortingTests;
import com.zavtech.morpheus.reference.StatsExpandingTests;
import com.zavtech.morpheus.reference.StatsRollingTests;
import com.zavtech.morpheus.reference.StatsTest1;
import com.zavtech.morpheus.reference.StatsBasicTests;
import com.zavtech.morpheus.reference.StreamTests;
import com.zavtech.morpheus.reference.StructureTests;
import com.zavtech.morpheus.reference.UpdateTests;
import com.zavtech.morpheus.io.CsvTests;
import com.zavtech.morpheus.io.JsonTests;
import com.zavtech.morpheus.range.RangeFilterTests;
import com.zavtech.morpheus.range.RangeBasicTests;
import com.zavtech.morpheus.reference.WLSTests;
import com.zavtech.morpheus.util.FormatsTest;
import com.zavtech.morpheus.util.SortAlgorithmTests;
import com.zavtech.morpheus.util.Tuple;
import com.zavtech.morpheus.util.TupleTests;

import org.testng.annotations.Factory;

/**
 * One line summary here...
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class TestSuite {


    /**
     * Constructor
     */
    public TestSuite() {
        super();
    }

    /**
     * Returns the directory for test output
     * @return  directory for test output
     */
    public static File getOutputDir(String testName) {
        final String tmpDir = System.getProperty("java.io.tmpdir");
        return new File(tmpDir,  "morpheus-tests/" + testName);
    }


    /**
     * Returns the directory for test output
     * @return  directory for test output
     */
    public static File getOutputFile(String testName, String fileName) {
        return new File(getOutputDir(testName), fileName);
    }


    /**
     * Returns the UK ONS population dataset
     * @return  the UK ONS population dataset
     */
    public static DataFrame<Tuple,String> getPopulationDataset() {
        return DataFrame.read().csv(options -> {
            options.setResource("http://tinyurl.com/ons-population-year");
            options.setRowKeyParser(Tuple.class, row -> Tuple.of(Integer.parseInt(row[1]), row[2]));
            options.setExcludeColumns("Code");
            options.getFormats().setNullValues("-");
            options.setFormats(formats -> {
                formats.setNullValues("-");
                formats.copyParser(Double.class, "All Males");
                formats.copyParser(Double.class, "All Females");
                formats.copyParser(Double.class, "All Persons");
                formats.copyParser(Double.class, "[MF]\\s+\\d+");
            });
        });
    }

}
