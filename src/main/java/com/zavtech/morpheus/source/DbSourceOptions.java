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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import javax.sql.DataSource;

import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.util.sql.SQLExtractor;

/**
 * A DataFrameRequest used to load a DataFrame from a SQL Database.
 *
 * @param <R>   the row key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class DbSourceOptions<R> implements DataFrameSource.Options<R,String> {

    private String sql;
    private int rowCapacity;
    private Connection connection;
    private Object[] parameters;
    private Set<String> excludeColumnSet;
    private Function<ResultSet,R> rowKeyFunction;
    private Map<String,SQLExtractor> extractorMap;

    /**
     * Constructor
     */
    @SuppressWarnings("unchecked")
    public DbSourceOptions() {
        this.rowCapacity = 1000;
        this.excludeColumnSet = new HashSet<>();
        this.extractorMap = new HashMap<>();
        this.rowKeyFunction = (ResultSet rs) -> {
            try {
                return (R)rs.getObject(1);
            } catch (SQLException ex) {
                throw new RuntimeException("Failed to read row key from SQL ResultSet", ex);
            }
        };
    }

    @Override
    public void validate() {
        Objects.requireNonNull(sql, "The SQL statement cannot be null");
        Objects.requireNonNull(connection, "The JDBC connection cannot be null");
    }

    /**
     * Sets the SQL query for this request
     * @param sql   the sql for this request
     * @return      this request
     */
    public DbSourceOptions<R> withSql(String sql) {
        Objects.requireNonNull(sql, "The sql cannot be null");
        this.sql = sql;
        return this;
    }

    /**
     * Sets the parameters for the SQL expression
     * @param parameters  the SQL parameters
     * @return      this request
     */
    public DbSourceOptions<R> withParameters(Object...parameters) {
        Objects.requireNonNull(parameters, "The sql parameters cannot be null, empty array is fine");
        this.parameters = parameters;
        return this;
    }

    /**
     * Sets the JDBC connection for this request
     * @param connection    the connection for this request
     * @return              this request
     */
    public DbSourceOptions<R> withConnection(Connection connection) {
        Objects.requireNonNull(connection, "The SQL connection cannot be null");
        this.connection = connection;
        return this;
    }

    /**
     * Sets the JDBC connection for this request
     * @param dataSource    the DataSource the grab a connection from
     * @return              this request
     */
    public DbSourceOptions<R> withConnection(DataSource dataSource) {
        Objects.requireNonNull(dataSource, "The SQL data source cannot be null");
        try {
            this.connection = dataSource.getConnection();
            return this;
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
     * @return          this request
     */
    public DbSourceOptions<R> withConnection(String url, String username, String password) {
        try {
            Objects.requireNonNull(url, "The JDBC URL cannnot be null");
            this.connection = DriverManager.getConnection(url, username, password);
            return this;
        } catch (SQLException ex) {
            throw new DataFrameException("Failed to create connection for URL:" + url, ex);
        }
    }

    /**
     * Sets the names of columns not to include in the resulting DataFrame
     * @param columns   the columns to exlcude from DataFrame
     * @return          this request
     */
    public DbSourceOptions<R> withExcludeColumns(String... columns) {
        this.excludeColumnSet.addAll(Arrays.asList(columns));
        return this;
    }

    /**
     * Sets the row key function for this request
     * @param rowKeyFunction    the row key function
     * @return                  this request
     */
    public DbSourceOptions<R> withRowKeyFunction(Function<ResultSet,R> rowKeyFunction) {
        Objects.requireNonNull(rowKeyFunction, "The row key function cannot be null");
        this.rowKeyFunction = rowKeyFunction;
        return this;
    }

    /**
     * Sets the initial row capacity to size the DataFrame
     * @param rowCapacity   the initial roe capacity
     * @return  this request
     */
    public DbSourceOptions<R> withRowCapacity(int rowCapacity) {
        this.rowCapacity = rowCapacity;
        return this;
    }

    /**
     * Sets the extractor to use for the column name
     * @param colName   the JDBC column name
     * @param extractor the extractor to use for column
     * @return          this reader
     */
    public DbSourceOptions<R> withExtractor(String colName, SQLExtractor extractor) {
        Objects.requireNonNull(colName, "The column name cannot be null");
        Objects.requireNonNull(extractor, "The database extractor");
        this.extractorMap.put(colName, extractor);
        return this;
    }

    /**
     * Returns the SQL query for this request
     * @return  the sql query
     */
    public String getSql() {
        return sql;
    }

    /**
     * Returns the parameters for this request
     * @return  the parameters for request
     */
    Optional<Object[]> getParameters() {
        return Optional.ofNullable(parameters);
    }

    /**
     * Returns the connection for this request
     * @return  the connection for this request
     */
    Connection getConnection() {
        return connection;
    }

    /**
     * Returns the initial row capacity to size the resulting DataFrame
     * @return  the row capacity
     */
    int getRowCapacity() {
        return rowCapacity;
    }

    /**
     * Returns the set of columns to exclude
     * @return  the set of columns to exclude
     */
    Set<String> getExcludeColumnSet() {
        return excludeColumnSet;
    }

    /**
     * Returns the row key generating function for this request
     * @return  the row query generating function
     */
    Function<ResultSet,R> getRowKeyFunction() {
        return rowKeyFunction;
    }

    /**
     * Returns the apply of extractors for this request
     * @return  the apply of extractors
     */
    Map<String,SQLExtractor> getExtractors() {
        return Collections.unmodifiableMap(extractorMap);
    }

}
