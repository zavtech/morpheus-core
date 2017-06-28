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

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameVector;
import org.testng.Assert;

/**
 * Helper class for DataFrame unit tests
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class DataFrameAsserts {

    /**
     * Asserts two frames are equal in terms of structure and values.
     * @param expected      the expected expected
     * @param actual        the actual frame
     */
    public static void assertEquals(DataFrameVector<?,?,?,?,?> expected, DataFrameVector<?,?,?,?,?> actual) {
        Assert.assertEquals(actual.size(), expected.size(), "The DataFrameVector sizes match");
        Assert.assertEquals(actual.key(), expected.key(), "The DataFrameVector keys match");
        for (int i=0; i<expected.size(); ++i) {
            final Object expectedValue = expected.getValue(i);
            final Object actualValue = actual.getValue(i);
            Assert.assertEquals(actualValue, expectedValue, "The DataFrame values match at (" + i + "," + i + ")");
        }
    }


    public static <R,C> void assertEqualStructure(DataFrame<R,C> expected, DataFrame<R,C> actual) {
        Assert.assertEquals(actual.rowCount(), expected.rowCount(), "The DataFrame row counts match");
        Assert.assertEquals(actual.colCount(), expected.colCount(), "The DataFrame column count match");
        Assert.assertEquals(actual.rows().keyType(), expected.rows().keyType(), "The DataFrame row key types match");
        Assert.assertEquals(actual.cols().keyType(), expected.cols().keyType(), "The DataFrame column key types match");
        for (int i=0; i<actual.rowCount(); ++i) {
            final Object rowKeyActual = actual.rows().key(i);
            final Object rowKeyExpected = expected.rows().key(i);
            Assert.assertEquals(rowKeyActual, rowKeyExpected, "DataFrame row keys matched at index " + i);
        }
        for (int i = 0; i<actual.colCount(); ++i) {
            final Object colKeyActual = actual.cols().key(i);
            final Object colKeyExpected = expected.cols().key(i);
            Assert.assertEquals(colKeyActual, colKeyExpected, "DataFrame column keys matched at index " + i);
        }
    }


    /**
     * Asserts two frames are equal in terms of structure and values based on indexes
     * @param expected      the expected expected
     * @param actual        the actual frame
     */
    @SuppressWarnings("unchecked")
    public static void assertEqualsByIndex(DataFrame<?,?> expected, DataFrame<?,?> actual) {
        DataFrameAsserts.assertEqualStructure((DataFrame<Object,Object>)expected, (DataFrame<Object,Object>)actual);
        for (int j = 0; j<expected.colCount(); ++j) {
            for (int i=0; i<expected.rowCount(); ++i) {
                final Object expectedValue = expected.data().getValue(i,j);
                final Object actualValue = actual.data().getValue(i, j);
                if (expectedValue != null && actualValue != null) {
                    final Class type1 = expectedValue.getClass();
                    final Class type2 = actualValue.getClass();
                    Assert.assertEquals(type2, type1, "The DataFrame value types match at (" + i + "," + j + ")");
                    if (type1.equals(Double.class)) {
                        final double v1 = ((Number)expectedValue).doubleValue();
                        final double v2 = ((Number)actualValue).doubleValue();
                        if (!Double.isNaN(v1) || !Double.isNaN(v2)) {
                            Assert.assertEquals(v2, v1, 0.00000001, "The DataFrame doubles match at (" + i + "," + j + ")");
                        }
                    } else {
                        Assert.assertEquals(actualValue, expectedValue, "The DataFrame values match at (" + i + "," + j + ")");
                    }
                } else {
                    Assert.assertEquals(actualValue, expectedValue, "The DataFrame values match at (" + i + "," + j + ")");
                }
            }
        }
    }


    /**
     * Asserts two frames are equal in terms of structure and values based on keys
     * @param expected      the expected expected
     * @param actual        the actual frame
     */
    static <R,C> void assertEqualsByKey(DataFrame<R,C> expected, DataFrame<R,C> actual) {
        Assert.assertEquals(actual.rowCount(), expected.rowCount(), "The DataFrame row counts match");
        Assert.assertEquals(actual.colCount(), expected.colCount(), "The DataFrame column count match");
        for (int i=0; i<expected.rowCount(); ++i) {
            final R expectedRowKey = expected.rows().key(i);
            Assert.assertTrue(actual.rows().contains(expectedRowKey), "DataFrame contains expected row key " + expectedRowKey);
        }
        for (int i = 0; i<expected.colCount(); ++i) {
            final C expectedColKey = expected.cols().key(i);
            Assert.assertTrue(actual.cols().contains(expectedColKey), "DataFrame contains expected column " + expectedColKey);
        }
        expected.rows().keys().forEach(rowKey -> {
            expected.cols().keys().forEach(colKey -> {
                final Object expectedValue = expected.data().getValue(rowKey, colKey);
                final Object actualValue = actual.data().getValue(rowKey, colKey);
                Assert.assertEquals(actualValue, expectedValue, "The DataFrame values match at (" + rowKey + "," + colKey + ")");
            });
        });
    }



}
