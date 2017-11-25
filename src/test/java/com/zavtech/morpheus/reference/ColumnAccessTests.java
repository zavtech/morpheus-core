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
import com.zavtech.morpheus.frame.DataFrameAsserts;
import com.zavtech.morpheus.frame.DataFrameColumn;

import junit.framework.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests reading/writing values by column
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class ColumnAccessTests {

    @DataProvider(name="args3")
    public Object[][] getArgs3() {
        return new Object[][] {
                { boolean.class },
                { int.class },
                { long.class },
                { double.class },
                { Object.class },
        };
    }


    @Test(dataProvider="args3")
    public void testAccessByColumnAndIndex(Class<?> type) {
        final DataFrame<String,String> source = TestDataFrames.random(type, 100, 100);
        final DataFrame<String,String> target = source.copy().applyValues(v -> null);
        if (type == boolean.class) {
            source.cols().forEach(column -> {
                final DataFrameColumn targetColumn = target.col(column.key());
                for (int i = 0; i < source.rowCount(); ++i) {
                    targetColumn.setBoolean(i, column.getBoolean(i));
                }
            });
        } else if (type == int.class) {
            source.cols().forEach(column -> {
                final DataFrameColumn targetColumn = target.col(column.key());
                for (int i = 0; i < source.rowCount(); ++i) {
                    targetColumn.setInt(i, column.getInt(i));
                }
            });
        } else if (type == long.class) {
            source.cols().forEach(column -> {
                final DataFrameColumn targetColumn = target.col(column.key());
                for (int i = 0; i < source.rowCount(); ++i) {
                    targetColumn.setLong(i, column.getLong(i));
                }
            });
        } else if (type == double.class) {
            source.cols().forEach(column -> {
                final DataFrameColumn targetColumn = target.col(column.key());
                for (int i = 0; i < source.rowCount(); ++i) {
                    targetColumn.setDouble(i, column.getDouble(i));
                }
            });
        } else if (type == Object.class) {
            source.cols().forEach(column -> {
                final DataFrameColumn targetColumn = target.col(column.key());
                for (int i = 0; i < source.rowCount(); ++i) {
                    targetColumn.setValue(i, column.getValue(i));
                }
            });
        } else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
        DataFrameAsserts.assertEqualsByIndex(source, target);
    }


    @Test(dataProvider="args3")
    public void testAccessByColumnAndKey(Class<?> type) {
        final DataFrame<String,String> source = TestDataFrames.random(type, 100, 100);
        final DataFrame<String,String> target = source.copy().applyValues(v -> null);
        if (type == boolean.class) {
            source.cols().forEach(column -> {
                Assert.assertTrue(column.isColumn());
                final DataFrameColumn<String, String> targetColumn = target.col(column.key());
                source.rows().keys().forEach(key -> targetColumn.setBoolean(key, column.getBoolean(key)));
            });
        } else if (type == int.class) {
            source.cols().forEach(column -> {
                Assert.assertTrue(column.isColumn());
                final DataFrameColumn<String, String> targetColumn = target.col(column.key());
                source.rows().keys().forEach(key -> targetColumn.setInt(key, column.getInt(key)));
            });
        } else if (type == long.class) {
            source.cols().forEach(column -> {
                Assert.assertTrue(column.isColumn());
                final DataFrameColumn<String, String> targetColumn = target.col(column.key());
                source.rows().keys().forEach(key -> targetColumn.setLong(key, column.getLong(key)));
            });
        } else if (type == double.class) {
            source.cols().forEach(column -> {
                Assert.assertTrue(column.isColumn());
                final DataFrameColumn<String, String> targetColumn = target.col(column.key());
                source.rows().keys().forEach(key -> targetColumn.setDouble(key, column.getDouble(key)));
            });
        } else if (type == Object.class) {
            source.cols().forEach(column -> {
                Assert.assertTrue(column.isColumn());
                final DataFrameColumn<String, String> targetColumn = target.col(column.key());
                source.rows().keys().forEach(key -> targetColumn.setValue(key, column.getValue(key)));
            });
        } else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
        DataFrameAsserts.assertEqualsByIndex(source, target);
    }

}
