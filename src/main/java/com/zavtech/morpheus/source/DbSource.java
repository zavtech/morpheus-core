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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBuilder;
import com.zavtech.morpheus.array.ArrayType;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.util.sql.SQL;
import com.zavtech.morpheus.util.sql.SQLExtractor;
import com.zavtech.morpheus.util.sql.SQLPlatform;
import com.zavtech.morpheus.util.sql.SQLType;

/**
 * A DataFrameSource designed to handle read DataFrames from a SQL data store
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class DbSource<R> extends DataFrameSource<R,String,DbSourceOptions<R>> {


    /**
     * Static initializer
     */
    static {
        DataFrameSource.register(new DbSource<>());
    }


    /**
     * Constructor
     */
    public DbSource() {
        super();
    }


    @Override
    public DataFrame<R, String> read(Consumer<DbSourceOptions<R>> configurator) throws DataFrameException {
        final DbSourceOptions<R> options = initOptions(new DbSourceOptions<>(), configurator);
        try (Connection conn = options.getConnection()) {
            final SQL sql = SQL.of(options.getSql(), options.getParameters().orElse(new Object[0]));
            return sql.executeQuery(conn, rs -> read(rs, options));
        } catch (Exception ex) {
            throw new DataFrameException("Failed to create DataFrame from database request: " + options, ex);
        }
    }


    /**
     * Reads all data from the sql ResultSet into a Morpheus DataFrame
     * @param resultSet     the result set to extract data from
     * @param request       the request descriptor
     * @return              the newly created DataFrame
     * @throws DataFrameException if data frame construction from result set fails
     */
    @SuppressWarnings("unchecked")
    private DataFrame<R,String> read(ResultSet resultSet, DbSourceOptions<R> request) throws DataFrameException {
        try {
            final int rowCapacity = request.getRowCapacity();
            final SQLPlatform platform = getPlatform(resultSet);
            final ResultSetMetaData metaData = resultSet.getMetaData();
            final List<ColumnInfo> columnList = getColumnInfo(metaData, platform, request);
            final Function<ResultSet,R> rowKeyFunction = request.getRowKeyFunction();
            if (!resultSet.next()) {
                final Index<R> rowKeys = Index.empty();
                return createFrame(rowKeys, columnList);
            } else {
                R rowKey = rowKeyFunction.apply(resultSet);
                final Class<R> rowKeyType = (Class<R>) rowKey.getClass();
                final ArrayBuilder<R> rowKeyBuilder = ArrayBuilder.of(rowCapacity, rowKeyType);
                while (true) {
                    rowKey = rowKeyFunction.apply(resultSet);
                    rowKeyBuilder.add(rowKey);
                    for (ColumnInfo colInfo : columnList) {
                        colInfo.apply(resultSet);
                    }
                    if (!resultSet.next()) {
                        break;
                    }
                }
                final Array<R> rowKeys = rowKeyBuilder.toArray();
                return createFrame(rowKeys, columnList);
            }
        } catch (DataFrameException ex) {
            throw ex;
        } catch (Throwable t) {
            throw new DataFrameException("Failed to initialize DataFrame from ResultSet: " + t.getMessage(), t);
        } finally {
            close(resultSet);
        }
    }


    /**
     * Returns the database platform type from the ResultSet
     * @param resultSet the result set
     * @return          the database type
     */
    private SQLPlatform getPlatform(ResultSet resultSet) {
        try {
            final DatabaseMetaData metaData = resultSet.getStatement().getConnection().getMetaData();
            final String driverClassName = metaData.getDriverName();
            return SQLPlatform.getPlatform(driverClassName);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to detect database platform type, please use withPlatform() on request", ex);
        }
    }


    /**
     * Returns a newly created DataFrame from the arguments specified
     * @param rowKeys       the row keys
     * @param columnList    the column list
     * @return              the newly created DataFrame
     */
    private DataFrame<R,String> createFrame(Iterable<R> rowKeys, List<ColumnInfo> columnList) {
        return DataFrame.of(rowKeys, String.class, columns -> {
            for (ColumnInfo colInfo : columnList) {
                final String colName = colInfo.name;
                final Array<?> values = colInfo.array.toArray();
                columns.add(colName, values);
            }
        });
    }


    /**
     * Returns the array of column information from the result-set meta-data
     * @param metaData      the result set meta data
     * @param platform      the database platform
     * @param request       the request descriptor
     * @return              the array of column information
     * @throws SQLException if there is a database access error
     */
    private List<ColumnInfo> getColumnInfo(ResultSetMetaData metaData, SQLPlatform platform, DbSourceOptions<R> request) throws SQLException {
        final int rowCapacity = request.getRowCapacity();
        final int columnCount = metaData.getColumnCount();
        final List<ColumnInfo> columnInfoList = new ArrayList<>(columnCount);
        final SQLType.TypeResolver typeResolver = SQLType.getTypeResolver(platform);
        for (int i=0; i<columnCount; ++i) {
            final int colIndex = i+1;
            final String colName = metaData.getColumnName(colIndex);
            if (!request.getExcludeColumnSet().contains(colName)) {
                final int typeCode = metaData.getColumnType(colIndex);
                final String typeName = metaData.getColumnTypeName(colIndex);
                final SQLType sqlType = typeResolver.getType(typeCode, typeName);
                final SQLExtractor extractor = request.getExtractors().getOrDefault(colName, SQLExtractor.with(sqlType.typeClass(), platform));
                columnInfoList.add(new ColumnInfo(i, colIndex, colName, rowCapacity, extractor));
            }
        }
        return columnInfoList;
    }


    /**
     * Safely closes the JDBC resource
     * @param closeable the closeable
     */
    private void close(AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }



    /**
     * A class used to capture the meta-data for a column
     */
    private class ColumnInfo {

        int index;
        int ordinal;
        String name;
        Class<?> type;
        ArrayType typeCode;
        SQLExtractor extractor;
        ArrayBuilder<Object> array;


        /**
         * Constructor
         * @param ordinal   the DataFrame column ordinal
         * @param index     the JDBC column index
         * @param name      the JDBC column name
         * @param capacity  the initial capacity for column
         */
        @SuppressWarnings("unchecked")
        ColumnInfo(int ordinal, int index, String name, int capacity, SQLExtractor extractor) {
            this.index = index;
            this.ordinal = ordinal;
            this.name = name;
            this.type = extractor.getDataType();
            this.typeCode = ArrayType.of(type);
            this.array = (ArrayBuilder<Object>)ArrayBuilder.of(capacity, type);
            this.extractor = extractor;
        }

        /**
         * Applies the ResultSet to this column for current row
         * @param rs    the ResultSet reference
         * @throws SQLException if database access error
         */
        final void apply(ResultSet rs) throws SQLException {
            try {
                switch (typeCode) {
                    case BOOLEAN:   array.addBoolean(extractor.getBoolean(rs, index));  break;
                    case INTEGER:   array.addInt(extractor.getInt(rs, index));          break;
                    case LONG:      array.addLong(extractor.getLong(rs, index));        break;
                    case DOUBLE:    array.addDouble(extractor.getDouble(rs, index));    break;
                    default:        array.add(extractor.getValue(rs, index));           break;
                }
            } catch (Exception ex) {
                throw new RuntimeException("Failed to extract data for column " + name, ex);
            }
        }
    }


}
