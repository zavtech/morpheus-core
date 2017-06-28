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

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayType;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameCursor;

/**
 * A utility class with some useful DataFrame related functions
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrameCopy {


    /**
     * Copies values from the source frame to the target based for the rows and column keys specified
     * @param source        the source frame to read from
     * @param target        the target frame to write to
     * @param rowKeys       the intersecting row keys between the two frames
     * @param colKeys       the intersecting column keys between the two frames
     * @param <R>           the row key type
     * @param <C>           the column key type
     * @return              the target frame
     */
    static <R,C> DataFrame<R,C> apply(XDataFrame<R,C> source, XDataFrame<R,C> target, Array<R> rowKeys, Array<C> colKeys) {
        final DataFrameCursor<R,C> readCursor = source.cursor();
        final DataFrameCursor<R,C> writeCursor = target.cursor();
        final int[] readRowOrdinals = source.rowKeys().ordinals(rowKeys).toArray();
        final int[] writeRowOrdinals = target.rowKeys().ordinals(rowKeys).toArray();
        for (C colKey : colKeys) {
            readCursor.moveToColumn(colKey);
            writeCursor.moveToColumn(colKey);
            switch (ArrayType.of(source.cols().type(colKey))) {
                case BOOLEAN:
                    for (int i=0; i<rowKeys.length(); ++i) {
                        readCursor.moveToRow(readRowOrdinals[i]);
                        writeCursor.moveToRow(writeRowOrdinals[i]);
                        writeCursor.setBoolean(readCursor.getBoolean());
                    }
                    break;
                case INTEGER:
                    for (int i=0; i<rowKeys.length(); ++i) {
                        readCursor.moveToRow(readRowOrdinals[i]);
                        writeCursor.moveToRow(writeRowOrdinals[i]);
                        writeCursor.setInt(readCursor.getInt());
                    }
                    break;
                case LONG:
                    for (int i=0; i<rowKeys.length(); ++i) {
                        readCursor.moveToRow(readRowOrdinals[i]);
                        writeCursor.moveToRow(writeRowOrdinals[i]);
                        writeCursor.setLong(readCursor.getLong());
                    }
                    break;
                case DOUBLE:
                    for (int i=0; i<rowKeys.length(); ++i) {
                        readCursor.moveToRow(readRowOrdinals[i]);
                        writeCursor.moveToRow(writeRowOrdinals[i]);
                        writeCursor.setDouble(readCursor.getDouble());
                    }
                    break;
                case DATE:
                    for (int i=0; i<rowKeys.length(); ++i) {
                        readCursor.moveToRow(readRowOrdinals[i]);
                        writeCursor.moveToRow(writeRowOrdinals[i]);
                        writeCursor.setLong(readCursor.getLong());
                    }
                    break;
                case LOCAL_DATE:
                    for (int i=0; i<rowKeys.length(); ++i) {
                        readCursor.moveToRow(readRowOrdinals[i]);
                        writeCursor.moveToRow(writeRowOrdinals[i]);
                        writeCursor.setLong(readCursor.getLong());
                    }
                    break;
                case LOCAL_TIME:
                    for (int i=0; i<rowKeys.length(); ++i) {
                        readCursor.moveToRow(readRowOrdinals[i]);
                        writeCursor.moveToRow(writeRowOrdinals[i]);
                        writeCursor.setLong(readCursor.getLong());
                    }
                    break;
                default:
                    for (int i=0; i<rowKeys.length(); ++i) {
                        readCursor.moveToRow(readRowOrdinals[i]);
                        writeCursor.moveToRow(writeRowOrdinals[i]);
                        writeCursor.setValue(readCursor.getValue());
                    }
                    break;
            }
        }
        return target;
    }
}
