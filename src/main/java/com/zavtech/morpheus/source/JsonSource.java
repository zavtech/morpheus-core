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
package com.zavtech.morpheus.source;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBuilder;
import com.zavtech.morpheus.array.ArrayType;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.util.Asserts;
import com.zavtech.morpheus.util.text.Formats;
import com.zavtech.morpheus.util.text.parser.Parser;

/**
 * A DataFrameSource implementation that services JsonRequests.
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class JsonSource <R,C> extends DataFrameSource<R,C,JsonSourceOptions<R,C>> {


    /**
     * Static initializer
     */
    static {
        DataFrameSource.register(new JsonSource<>());
    }


    private Formats formats;
    private JsonReader reader;
    private List<Array<?>> dataList;
    private ArrayBuilder<R> rowKeyBuilder;
    private ArrayBuilder<C> colKeyBuilder;
    private JsonSourceOptions<R,C> options;

    /**
     * Constructor
     */
    public JsonSource() {
        super();
    }


    @Override
    public DataFrame<R,C> read(Consumer<JsonSourceOptions<R, C>> configurator) throws DataFrameException {
        return new JsonSource<R,C>().apply(configurator);
    }

    /**
     * Private read method given this source is currently written with state
     * @param configurator      the options configurator
     * @return                  the loaded DataFrame
     * @throws DataFrameException   if frame fails to load from json
     */
    private DataFrame<R,C> apply(Consumer<JsonSourceOptions<R,C>> configurator) throws DataFrameException {
        this.options = initOptions(new JsonSourceOptions<>(), configurator);
        try (InputStream is = options.getResource().toInputStream()) {
            this.formats = options.getFormats();
            this.reader = createReader(is);
            this.reader.beginObject();
            if (reader.hasNext()) {
                final String rootName = reader.nextName();
                if (!rootName.equalsIgnoreCase("DataFrame")) {
                    throw new DataFrameException("Unsupported JSON format for DataFrame");
                } else {
                    reader.beginObject();
                    int rowCount = -1;
                    int colCount = -1;
                    while (reader.hasNext()) {
                        final String name = reader.nextName();
                        if (name.equalsIgnoreCase("rowCount")) {
                            rowCount = reader.nextInt();
                        } else if (name.equalsIgnoreCase("colCount")) {
                            colCount = reader.nextInt();
                        } else if (name.equalsIgnoreCase("rowKeys")) {
                            readRowKeys(rowCount);
                        } else if (name.equalsIgnoreCase("columns")) {
                            readColumns(rowCount, colCount);
                        }
                    }
                }
            }
            reader.endObject();
            return createFrame();
        } catch (DataFrameException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new DataFrameException("Failed to load DataFrame for request: " + options, ex);
        }
    }

    /**
     * Returns a newly created DataFrame from the contents that has been read from JSON
     * @return      the newly created DataFrame
     */
    private DataFrame<R,C> createFrame() {
        final Array<R> rowKeys = rowKeyBuilder.toArray();
        final Array<C> colKeys = colKeyBuilder.toArray();
        final Class<C> colType = colKeys.type();
        return DataFrame.of(rowKeys, colType, columns -> {
            for (int i=0; i<colKeys.length(); ++i) {
                final C colKey = colKeys.getValue(i);
                final Array<?> array = dataList.get(i);
                columns.add(colKey, array);
            }
        });
    }

    /**
     * Returns a newly created JsonReader
     * @param is        the input stream
     * @return          the JsonReader
     * @throws IOException  if there is an I/O exception
     */
    private JsonReader createReader(InputStream is) throws IOException {
        final String encoding = options.getCharset().name();
        if (is instanceof BufferedInputStream) {
            return new JsonReader(new InputStreamReader(is, encoding));
        } else {
            return new JsonReader(new InputStreamReader(new BufferedInputStream(is), encoding));
        }
    }

    /**
     * Reads row keys from the stream
     * @param rowCount  the row count for frame
     */
    @SuppressWarnings("unchecked")
    private void readRowKeys(int rowCount) {
        try {
            reader.beginObject();
            rowKeyBuilder = ArrayBuilder.of(rowCount);
            Parser<R> parser = formats.getParserOrFail(Object.class);
            final Map<String,Class<?>> typeMap = getTypeMap(formats);
            while (reader.hasNext()) {
                final String name = reader.nextName();
                if (name.equalsIgnoreCase("type")) {
                    final String typeName = reader.nextString();
                    final Class<?> dataType = typeMap.get(typeName);
                    if (dataType == null) throw new IllegalArgumentException("No Formats parser exists for type name: " + typeName);
                    parser = formats.getParserOrFail(typeName, dataType);
                } else if (name.equalsIgnoreCase("values")) {
                    reader.beginArray();
                    while (reader.hasNext()) {
                        final String value = reader.nextString();
                        final R key = parser.apply(value);
                        rowKeyBuilder.add(key);
                    }
                    reader.endArray();
                }
            }
            reader.endObject();
        } catch (DataFrameException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new DataFrameException("Failed to read row keys for DataFrame", ex);
        }
    }

    /**
     * Reads the column data from JSON and returs a apply of column data by key
     * @param rowCount      the row count
     * @param colCount      the column count
     */
    @SuppressWarnings("unchecked")
    private void readColumns(int rowCount, int colCount) {
        try {
            this.reader.beginArray();
            this.dataList = new ArrayList<>(colCount);
            this.colKeyBuilder = ArrayBuilder.of(colCount);
            final Map<String,Class<?>> typeMap = getTypeMap(formats);
            while (reader.hasNext()) {
                reader.beginObject();
                String key = null;
                boolean sparse = false;
                ArrayType dataType = null;
                Class<?> dataClass = null;
                String keyTypeName = null;
                Function<String,?> parser = formats.getParserOrFail(Object.class);
                ArrayBuilder<Object> dataBuilder = null;
                while (reader.hasNext()) {
                    final String name = reader.nextName();
                    if (name.equalsIgnoreCase("key")) {
                        key = reader.nextString();
                    } else if (name.equalsIgnoreCase("keyType")) {
                        keyTypeName = reader.nextString();
                        final Class<?> keyType = typeMap.get(keyTypeName);
                        final C colKey = (C)formats.getParserOrFail(keyType).apply(key);
                        this.colKeyBuilder.add(colKey);
                    } else if (name.equalsIgnoreCase("dataType")) {
                        final String dataTypeName = reader.nextString();
                        dataClass = typeMap.get(dataTypeName);
                        parser = formats.getParserOrFail(dataTypeName, dataClass);
                        if (parser == null) throw new RuntimeException("No parser configured in formats for type named: " + dataTypeName);
                        dataType = ArrayType.of(dataClass);
                    } else if (name.equalsIgnoreCase("sparse")) {
                        sparse = reader.nextBoolean();
                    } else if (name.equalsIgnoreCase("defaultValue")) {
                        Asserts.notNull(dataType, "The data type for column has not been resolved: " + key);
                        final JsonToken token = reader.peek();
                        if (token == JsonToken.NULL) {
                            reader.nextNull();
                            dataBuilder = ArrayBuilder.of(rowCount, (Class<Object>)dataClass, null);
                        } else if (token == JsonToken.BOOLEAN) {
                            final boolean nullValue = token != JsonToken.NULL && reader.nextBoolean();
                            dataBuilder = ArrayBuilder.of(rowCount, (Class<Object>)dataClass, nullValue);
                        } else if (token == JsonToken.NUMBER) {
                            final String nullString = token == JsonToken.NULL ? null : reader.nextString();
                            dataBuilder = ArrayBuilder.of(rowCount, (Class<Object>)dataClass, parser.apply(nullString));
                        } else if (token == JsonToken.STRING) {
                            final String nullString = token == JsonToken.NULL ? null : String.valueOf(reader.nextDouble());
                            dataBuilder = ArrayBuilder.of(rowCount, (Class<Object>)dataClass, parser.apply(nullString));
                        } else {
                            throw new IllegalStateException("Unexpected JsonToken: " + token);
                        }
                    } else if (name.equalsIgnoreCase("values")) {
                        Asserts.notNull(dataType, "The data type for column has not been resolved: " + key);
                        Asserts.notNull(dataBuilder, "");
                        reader.beginArray();
                        if (dataType.isBoolean()) {
                            while (reader.hasNext()) {
                                final boolean isNull = reader.peek() == JsonToken.NULL;
                                final boolean rawValue = !isNull && reader.nextBoolean();
                                dataBuilder.addBoolean(rawValue);
                            }
                        } else if (dataType.isInteger()) {
                            while (reader.hasNext()) {
                                final boolean isNull = reader.peek() == JsonToken.NULL;
                                final int rawValue = isNull ? 0 : reader.nextInt();
                                dataBuilder.addInt(rawValue);
                            }
                        } else if (dataType.isLong()) {
                            while (reader.hasNext()) {
                                final boolean isNull = reader.peek() == JsonToken.NULL;
                                final long rawValue = isNull ? 0L : reader.nextLong();
                                dataBuilder.addLong(rawValue);
                            }
                        } else if (dataType.isDouble()) {
                            while (reader.hasNext()) {
                                final boolean isNull = reader.peek() == JsonToken.NULL;
                                final double rawValue = isNull ? Double.NaN : reader.nextDouble();
                                dataBuilder.addDouble(rawValue);
                            }
                        } else {
                            while (reader.hasNext()) {
                                final boolean isNull = reader.peek() == JsonToken.NULL;
                                final String rawValue = isNull ? null : reader.nextString();
                                final Object value = parser.apply(rawValue);
                                dataBuilder.add(value);
                            }
                        }
                        final Array<?> array = dataBuilder.toArray();
                        this.dataList.add(array);
                        this.reader.endArray();
                    }
                }
                reader.endObject();
            }
            reader.endArray();
        } catch (DataFrameException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new DataFrameException("Failed to read columns for DataFrame", ex);
        }
    }


    /**
     * Returns the type apply from the formats
     * @param formats   the formats
     * @return          the type apply
     */
    private Map<String,Class<?>> getTypeMap(Formats formats) {
        final Map<String,Class<?>> typeMap = new HashMap<>();
        formats.getParserKeys().forEach(key -> {
            if (key instanceof Class) {
                final Class<?> type = (Class<?>)key;
                final String name = type.getSimpleName();
                typeMap.put(name, type);
            }
        });
        return typeMap;
    }

}
