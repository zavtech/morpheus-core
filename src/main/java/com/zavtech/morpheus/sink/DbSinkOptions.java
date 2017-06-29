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

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.sql.DataSource;

import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameValue;
import com.zavtech.morpheus.util.Asserts;
import com.zavtech.morpheus.util.functions.Function1;
import com.zavtech.morpheus.util.sql.SQLPlatform;

/**
 * A class that defines all the options that can be configured to write a DataFrame to a SQL data store.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class DbSinkOptions<R,C> {

    private int batchSize;
    private String tableName;
    private Connection connection;
    private SQLPlatform platform;
    private ColumnMappings columnMappings;
    private String autoIncrementColumnName;
    private Function<C,String> columnNames;
    private String rowKeyColumn;
    private Class<?> rowKeySqlClass;
    private Function1<R,?> rowKeyMapper;

    /**
     * Constructor
     */
    public DbSinkOptions() {
        this.batchSize = 1000;
        this.columnNames = Object::toString;
        this.columnMappings = new ColumnMappings();
    }

    /**
     * Returns the batch size for inserts
     * @return  the batch size
     */
    int getBatchSize() {
        return batchSize;
    }

    /**
     * Returns the table name to write to
     * @return  the table name
     */
    String getTableName() {
        return tableName;
    }

    /**
     * Returns the SQL platform for these options
     * @return      the SQL platform
     */
    Optional<SQLPlatform> getPlatform() {
        return Optional.ofNullable(platform);
    }

    /**
     * Returns the connection for these options
     * @return  the database connection
     */
    Connection getConnection() {
        return connection;
    }

    /**
     * Returns the column name to store DataFrame row keys in
     * @return  the optional column to store row keys
     */
    Optional<String> getRowKeyColumn() {
        return Optional.ofNullable(rowKeyColumn);
    }

    /**
     * Returns the SQL type to apply row keys to
     * @return  the SQL type for row keys
     */
    Optional<Class<?>> getRowKeySqlClass() {
        return Optional.ofNullable(rowKeySqlClass);
    }

    /**
     * Returns the mapper that maps row keys to the appropriate JDBC type
     * @return      the row key mapper if specified
     */
    Optional<Function1<R,?>> getRowKeyMapper() {
        return Optional.ofNullable(rowKeyMapper);
    }

    /**
     * Returns the optional column name for the auto increment field
     * @return  the optional auto increment column name
     */
    Optional<String> getAutoIncrementColumnName() {
        return Optional.ofNullable(autoIncrementColumnName);
    }

    /**
     * Returns the column mappings for these options
     * @return      the column mappings
     */
    public ColumnMappings getColumnMappings() {
        return columnMappings;
    }

    /**
     * Returns the function that yield column names for DataFrame column keys
     * @return      the function that yields column names for frame column keys
     */
    Function<C,String> getColumnNames() {
        return columnNames;
    }

    /**
     * Sets the batch size for write operations
     * @param batchSize the batch size
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * Sets the target table name to write to
     * @param tableName the fully qualified table name
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Sets the JDBC connection for this request
     * @param connection    the connection for this request
     */
    public void setConnection(Connection connection) {
        Objects.requireNonNull(connection, "The SQL connection cannot be null");
        this.connection = connection;
    }

    /**
     * Sets the JDBC data source for this request
     * @param dataSource    the DataSource the grab a connection from
     */
    public void setConnection(DataSource dataSource) {
        Objects.requireNonNull(dataSource, "The SQL data source cannot be null");
        try {
            this.connection = dataSource.getConnection();
        } catch (SQLException ex) {
            throw new RuntimeException("Failed to access a DB connection from DataSource", ex);
        }
    }

    /**
     * Sets the JDBC connection URL and optional credentials
     * This method should ideally only be used for testing, a connection pool is preferable for production
     * @param url       the JDBC connection url
     * @param username  the JDBC connection username, can be null
     * @param password  the JDBC connection password, can be null
     */
    public void setConnection(String url, String username, String password) {
        try {
            Objects.requireNonNull(url, "The JDBC URL cannnot be null");
            this.connection = DriverManager.getConnection(url, username, password);
        } catch (SQLException ex) {
            throw new DataFrameException("Failed to create connection for URL:" + url, ex);
        }
    }

    /**
     * Sets the row key mapping for these database options
     * @param colName   the column to apply row keys to
     * @param sqlType   the target SQL type for row key column
     * @param mapper    the row key mapper function
     * @param <T>       the mapped row key type
     */
    public <T> void setRowKeyMapping(String colName, Class<T> sqlType, Function1<R,T> mapper) {
        this.rowKeyColumn = Asserts.notNull(colName, "The row key column name cannot be null");
        this.rowKeySqlClass = Asserts.notNull(sqlType, "The row key SQL type cannot be null");
        this.rowKeyMapper = Asserts.notNull(mapper, "The row key mapper cannot be null");
    }

    /**
     * Sets the name of an auto increment column name if one exists in the table
     * @param autoIncrementColumnName   the auto increment column name
     */
    public void setAutoIncrementColumnName(String autoIncrementColumnName) {
        this.autoIncrementColumnName = autoIncrementColumnName;
    }

    /**
     * Sets the target SQL platform for these options
     * @param platform      the target SQL platform
     */
    public void setPlatform(SQLPlatform platform) {
        this.platform = platform;
    }

    /**
     * Sets the column name generating function
     * @param columnNames   the column name generating function
     */
    public void setColumnNames(Function<C,String> columnNames) {
        this.columnNames = columnNames;
    }

    /**
     * Sets the column mappings for these options
     * @param configurator the configurator for column mappings
     */
    public void setColumnMappings(Consumer<ColumnMappings> configurator) {
        configurator.accept(columnMappings);
    }


    /**
     * A class that maintains a mapping between a DataFrame column type and a JDBC column type
     */
    public class ColumnMappings {

        private Map<Class<?>,Class<?>> sqlTypeMap = new HashMap<>();
        private Map<Class<?>,Function1<DataFrameValue<R,C>,?>> mapperMap = new HashMap<>();

        /**
         * Constructor
         */
        ColumnMappings() {
            this.add(Boolean.class, Boolean.class, Function1.toBoolean(DataFrameValue::getBoolean));
            this.add(Integer.class, Integer.class, Function1.toInt(DataFrameValue::getInt));
            this.add(Long.class, Long.class, Function1.toLong(DataFrameValue::getLong));
            this.add(Double.class, Double.class, Function1.toDouble(DataFrameValue::getDouble));
            this.add(String.class, String.class, Function1.toValue(DataFrameValue::<String>getValue));
            this.add(java.sql.Date.class, java.sql.Date.class, Function1.toValue(DataFrameValue::<Date>getValue));
            this.add(java.sql.Time.class, java.sql.Time.class, Function1.toValue(DataFrameValue::<Time>getValue));
            this.add(java.sql.Timestamp.class, java.sql.Timestamp.class, Function1.toValue(DataFrameValue::<Timestamp>getValue));
            this.add(java.util.Date.class, java.sql.Date.class, Function1.toValue(v -> new Date(v.<java.util.Date>getValue().getTime())));
            this.add(LocalTime.class, Time.class, Function1.toValue(v -> Time.valueOf(v.<LocalTime>getValue())));
            this.add(LocalDate.class, java.sql.Date.class, Function1.toValue(v -> Date.valueOf(v.<LocalDate>getValue())));
            this.add(LocalDateTime.class, Timestamp.class, Function1.toValue(v -> Timestamp.valueOf(v.<LocalDateTime>getValue())));
            this.add(ZonedDateTime.class, Timestamp.class, Function1.toValue(v -> Timestamp.valueOf(v.<ZonedDateTime>getValue().toLocalDateTime())));
        }

        /**
         * Returns the SQL type for the DataFrame type
         * @param dataType  the DataFrame column type
         * @return          the SQL type
         * @throws DataFrameException   if no match for data type
         */
        Class<?> getSqlType(Class<?> dataType) throws DataFrameException {
            final Class<?> sqlType = sqlTypeMap.get(dataType);
            if (sqlType != null) {
                return sqlType;
            } else if (dataType.isEnum()) {
                return String.class;
            } else {
                throw new DataFrameException("No SQL type mapped for data type: " + dataType.getSimpleName());
            }
        }

        /**
         * Returns the mapper function to transform DataFrame type into SQL type
         * @param dataType      the DataFrame column type class
         * @return              the SQL column type class
         */
        Function1<DataFrameValue<R,C>,?> getMapper(Class<?> dataType) {
            final Function1<DataFrameValue<R,C>,?> mapper = mapperMap.get(dataType);
            if (mapper != null) {
                return mapper;
            } else if (dataType.isEnum()) {
                return Function1.toValue(v -> ((Enum)v).name());
            } else {
                throw new DataFrameException("No SQL mapper function for data type: " + dataType.getSimpleName());
            }
        }

        /**
         * Adds a mapping between a DataFrame column type and the approprivate JDBC type
         * @param dataClass  the DataFrame column data type
         * @param sqlClass   the SQL data type supported by JDBC
         * @param mapper    the mapper function to transform A into B
         * @param <A>       the DataFrame column type
         * @param <B>       the JDBC type
         */
        public <A,B> void add(Class<A> dataClass, Class<B> sqlClass, Function1<DataFrameValue<R,C>,B> mapper) {
            Asserts.notNull(dataClass, "The data type cannot be null");
            Asserts.notNull(sqlClass, "The sql type cannot be null");
            Asserts.notNull(mapper, "The sql mapper function cannot be nul");
            this.sqlTypeMap.put(dataClass, sqlClass);
            this.mapperMap.put(dataClass, mapper);
        }
    }
}

