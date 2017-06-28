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
package com.zavtech.morpheus.sink;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.function.Consumer;

import com.google.gson.stream.JsonWriter;

import com.zavtech.morpheus.array.ArrayType;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameColumn;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameSink;
import com.zavtech.morpheus.util.Initialiser;
import com.zavtech.morpheus.util.text.Formats;
import com.zavtech.morpheus.util.text.printer.Printer;

/**
 * A DataFrameSink implementation that writes a DataFrame out in a Morpheus specific JSON format.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class JsonSink<R,C> implements DataFrameSink<R,C,JsonSinkOptions> {

    /**
     * Constructor
     */
    public JsonSink() {
        super();
    }


    @Override
    public void write(DataFrame<R,C> frame, Consumer<JsonSinkOptions> configurator) {
        JsonWriter writer = null;
        try {
            final JsonSinkOptions jsonOptions = Initialiser.apply(JsonSinkOptions.class, configurator);
            final String encoding = jsonOptions.getEncoding();
            final OutputStream os = jsonOptions.getResource().toOutputStream();
            writer = new JsonWriter(new OutputStreamWriter(os, encoding));
            writer.setIndent("  ");
            writer.beginObject();
            writer.name("DataFrame");
            writer.beginObject();
            writer.name("rowCount").value(frame.rowCount());
            writer.name("colCount").value(frame.colCount());
            writeRowKeys(frame, writer, jsonOptions);
            writerColumns(frame, writer, jsonOptions);
            writer.endObject();
            writer.endObject();
        } catch (Exception ex) {
            throw new DataFrameException("Failed to write DataFrame to JSON output", ex);
        } finally {
            try {
                if (writer != null) {
                    writer.flush();
                    writer.close();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    /**
     * Writes row keys to json writer
     * @param frame the frame reference
     */
    @SuppressWarnings("unchecked")
    private <R,C> void writeRowKeys(DataFrame<R,C> frame, JsonWriter writer, JsonSinkOptions options) {
        try {
            final Formats formats = options.getFormats();
            final Class keyType = frame.rows().keyType();
            writer.name("rowKeys");
            writer.beginObject();
            writer.name("type").value(keyType.getSimpleName());
            writer.name("values");
            writer.beginArray();
            final Printer<Object> format = formats.getPrinterOrFail(keyType, null);
            frame.rows().keys().forEach(rowKey -> {
                try {
                    writer.value(format.apply(rowKey));
                } catch (IOException ex) {
                    throw new DataFrameException("Failed to write DataFrame row key: " + rowKey, ex);
                }
            });
            writer.endArray();
            writer.endObject();
        } catch (DataFrameException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new DataFrameException("Failed to write row keys for DataFrame", ex);
        }
    }


    /**
     * Writes column data to the json writer
     * @param frame     the frame reference
     */
    private <R,C> void writerColumns(DataFrame<R,C> frame, JsonWriter writer, JsonSinkOptions options) {
        try {
            writer.name("columns");
            writer.beginArray();
            frame.cols().forEach(column -> {
                writeColumn(frame, column, writer, options);
            });
            writer.endArray();
        } catch (DataFrameException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new DataFrameException("Failed to write DataFrame columns to json", ex);
        }
    }

    /**
     * Writes data for a specific column to the json writer
     * @param frame     the frame reference
     * @param column    the column reference
     */
    @SuppressWarnings("unchecked")
    private <R,C> void writeColumn(DataFrame<R,C> frame, DataFrameColumn<R,C> column, JsonWriter writer, JsonSinkOptions options) {
        try {
            final C colKey = column.key();
            final Formats formats = options.getFormats();
            final Class<?> colType = frame.cols().type(colKey);
            final ArrayType type = ArrayType.of(colType);
            final Object defaultValue = ArrayType.defaultValue(colType);
            final Class<?> typeClass = frame.cols().type(colKey);
            final Printer<Object> keyPrinter = formats.getPrinterOrFail(colKey.getClass());
            final Printer<Object> valuePrinter = formats.getPrinterOrFail(colKey, typeClass);
            if (valuePrinter == null) {
                throw new IllegalStateException("No Formats printer for column: " + colKey);
            }
            writer.beginObject();
            writer.name("key").value(keyPrinter.apply(colKey));
            writer.name("keyType").value(colKey.getClass().getSimpleName());
            writer.name("dataType").value(typeClass.getSimpleName());
            if (type.isBoolean()) {
                final boolean defaultBoolean = defaultValue == null ? false : (Boolean)defaultValue;
                writer.name("defaultValue").value(defaultBoolean);
                writer.name("values").beginArray();
                column.forEachValue(v -> {
                    try {
                        writer.value(v.getBoolean());
                    } catch (IOException ex) {
                        throw new DataFrameException("Failed to write DataFrame values for column " + colKey, ex);
                    }
                });

            } else if (type.isInteger()) {
                final Number defaultNumber = defaultValue == null ? 0 : (Number) defaultValue;
                writer.name("defaultValue").value(defaultNumber);
                writer.name("values").beginArray();
                column.forEachValue(v -> {
                    try {
                        writer.value(v.getInt());
                    } catch (IOException ex) {
                        throw new DataFrameException("Failed to write DataFrame values for column " + colKey, ex);
                    }
                });
            } else if (type.isLong()) {
                    final Number defaultNumber = defaultValue == null ? 0 : (Number)defaultValue;
                    writer.name("defaultValue").value(defaultNumber);
                    writer.name("values").beginArray();
                    column.forEachValue(v -> {
                        try {
                            writer.value(v.getLong());
                        } catch (IOException ex) {
                            throw new DataFrameException("Failed to write DataFrame values for column " + colKey, ex);
                        }
                    });
            } else if (type.isDouble()) {
                final Number defaultNumber = defaultValue == null ? 0 : (Number)defaultValue;
                writer.name("defaultValue").value(Double.isNaN(defaultNumber.doubleValue()) ? null : defaultNumber);
                writer.name("values").beginArray();
                column.forEachValue(v -> {
                    try {
                        writer.value(v.getDouble());
                    } catch (IOException ex) {
                        throw new DataFrameException("Failed to write DataFrame values for column " + colKey, ex);
                    }
                });
            } else {
                writer.name("defaultValue").value(defaultValue == null ? null : valuePrinter.apply(defaultValue));
                writer.name("values").beginArray();
                column.forEachValue(v -> {
                    try {
                        final Object rawValue = v.getValue();
                        final String stringValue = valuePrinter.apply(rawValue);
                        writer.value(stringValue);
                    } catch (IOException ex) {
                        throw new DataFrameException("Failed to write DataFrame values for column " + colKey, ex);
                    }
                });
            }
            writer.endArray();
            writer.endObject();
        } catch (DataFrameException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new DataFrameException("Failed to write DataFrame values for column " + column.key(), ex);
        }
    }
}

