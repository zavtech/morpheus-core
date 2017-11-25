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
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameCursor;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameRow;
import com.zavtech.morpheus.frame.DataFrameSink;
import com.zavtech.morpheus.frame.DataFrameValue;
import com.zavtech.morpheus.util.Collect;
import com.zavtech.morpheus.util.Initialiser;
import com.zavtech.morpheus.util.functions.Function1;
import com.zavtech.morpheus.util.sql.SQLPlatform;
import com.zavtech.morpheus.util.sql.SQLType;

/**
 * A DataFrameSink implementation that writes DataFrames to a SQL database table.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class DbSink<R,C> implements DataFrameSink<R,C,DbSinkOptions<R,C>> {

    private static final Map<Class<?>,SQLType> sqlTypeMap = new HashMap<>();

    /**
     * Static initializer
     */
    static {
        sqlTypeMap.put(boolean.class, SQLType.BIT);
        sqlTypeMap.put(Boolean.class, SQLType.BIT);
        sqlTypeMap.put(int.class, SQLType.INTEGER);
        sqlTypeMap.put(Integer.class, SQLType.INTEGER);
        sqlTypeMap.put(long.class, SQLType.BIGINT);
        sqlTypeMap.put(Long.class, SQLType.BIGINT);
        sqlTypeMap.put(float.class, SQLType.DOUBLE);
        sqlTypeMap.put(Float.class, SQLType.DOUBLE);
        sqlTypeMap.put(double.class, SQLType.DOUBLE);
        sqlTypeMap.put(Double.class, SQLType.DOUBLE);
        sqlTypeMap.put(String.class, SQLType.VARCHAR);
        sqlTypeMap.put(java.sql.Time.class, SQLType.TIME);
        sqlTypeMap.put(java.sql.Date.class, SQLType.DATE);
        sqlTypeMap.put(java.sql.Timestamp.class, SQLType.DATETIME);
    }

    /**
     * Constructor
     */
    public DbSink() {
        super();
    }


    @Override()
    public void write(DataFrame<R,C> frame, Consumer<DbSinkOptions<R,C>> configurator) {
        Objects.requireNonNull(frame, "DataFrame cannot be null");
        Objects.requireNonNull(configurator, "The options consumer cannot be null");
        final DbSinkOptions<R,C> options = Initialiser.apply(new DbSinkOptions<>(), configurator);
        try (Connection conn = options.getConnection()) {
            if (!options.getPlatform().isPresent()) {
                final String driverName = conn.getMetaData().getDriverName();
                final SQLPlatform platform = SQLPlatform.getPlatform(driverName);
                options.setPlatform(platform);
            }
            this.createTable(frame, options);
            this.insertData(frame, options);
        } catch (Exception ex) {
            throw new DataFrameException("Failed to write DataFrame to database table " + options.getTableName(), ex);
        }
    }


    /**
     * Creates the target table if it does not already exist in the database
     * @param frame     the DataFrame to create a table for
     * @param options   the sink options
     * @throws DataFrameException   if this operation fails
     */
    private void createTable(DataFrame<R,C> frame, DbSinkOptions<R,C> options) {
        final Connection conn = options.getConnection();
        final String tableName = options.getTableName();
        try (Statement stmt = conn.createStatement()) {
            final DatabaseMetaData metaData = conn.getMetaData();
            final ResultSet tables = metaData.getTables(null, null, tableName, null);
            if (tables.next()) {
                System.out.println("The table named " + tableName + " already exists");
            } else {
                final String ddl = getCreateTableSql(frame, options);
                System.out.println("Executing DDL:\n " + ddl);
                stmt.executeUpdate(ddl);
            }
        } catch (Exception ex) {
            throw new DataFrameException("Failed to create table named " + tableName + " in database", ex);
        }
    }


    /**
     * Called to insert data from the DataFrame to the target table
     * @param frame     the DataFrame to load data from
     * @param options   the sink options
     * @throws DataFrameException   if this operation fails
     */
    private void insertData(DataFrame<R,C> frame, DbSinkOptions<R,C> options) {
        final List<ColumnAdapter> columnList = getColumnAdapters(frame, options);
        final String insertSql = getInsertSql(columnList, options);
        System.out.println("Insert SQL: " + insertSql);
        try (PreparedStatement stmt = options.getConnection().prepareStatement(insertSql)) {
            int rowCount = 0;
            for (DataFrameRow<R,C> row : frame.rows()) {
                for (int i=0; i<columnList.size(); ++i) {
                    final ColumnAdapter adapter = columnList.get(i);
                    final int stmtIndex = i+1;
                    adapter.apply(stmt, stmtIndex, row);
                }
                stmt.addBatch();
                rowCount++;
                if (rowCount % options.getBatchSize() == 0) {
                    System.out.println("Executing batch, row count is " + rowCount);
                    stmt.executeBatch();
                }
            }
            if (rowCount % options.getBatchSize() != 0) {
                System.out.println("Executing final batch, row count is " + rowCount);
                stmt.executeBatch();
            }
        } catch (Exception ex) {
            throw new DataFrameException("Failed to insert data from DataFrame into table named " + options.getTableName(), ex);
        }
    }



    /**
     * Constructs the SQL insert statement for the column list specified
     * @param columnList    the column list
     * @param options       the DB sink options
     * @return              the sql insert statement
     */
    private String getInsertSql(List<ColumnAdapter> columnList, DbSinkOptions<R,C> options) {
        final String tableName = options.getTableName();
        final List<String> colNames = columnList.stream().map(c -> "\"" + c.colName + "\"").collect(Collectors.toList());
        final List<String> params = IntStream.range(0, colNames.size()).mapToObj(i -> "?").collect(Collectors.toList());
        final String columnsString = String.join(",", colNames);
        final String paramsString = String.join(",", params);
        return String.format("INSERT INTO \"%s\" (%s) VALUES (%s)", tableName, columnsString, paramsString);
    }


    /**
     * Returns a apply of column type info for the target table
     * @param frame     the DataFrame reference
     * @return          the apply of column type info
     */
    @SuppressWarnings("unchecked")
    private List<ColumnAdapter> getColumnAdapters(DataFrame<R,C> frame, DbSinkOptions<R,C> options) {
        final Connection conn = options.getConnection();
        final String tableName = options.getTableName();
        final SQLPlatform platform = options.getPlatform().orElseThrow(() -> new IllegalStateException("No SQL platform specified in options"));
        final Map<C,String> columnMap1 = frame.cols().keys().collect(Collectors.toMap(c -> c, c -> options.getColumnNames().apply(c)));
        final Map<String,C> columnMap2 = Collect.reverse(columnMap1);
        try (Statement stmt = conn.createStatement()) {
            final String sql = "select * from \"" + tableName + "\" where 1=2";
            final List<ColumnAdapter> columnList = new ArrayList<>();
            final ResultSetMetaData metaData = stmt.executeQuery(sql).getMetaData();
            final SQLType.TypeResolver typeResolver = SQLType.getTypeResolver(platform);
            for (int i=0; i<metaData.getColumnCount(); ++i) {
                final String sqlColName = metaData.getColumnName(i+1);
                final int sqlTypeCode = metaData.getColumnType(i+1);
                final String sqlTypeName = metaData.getColumnTypeName(i+1);
                final SQLType sqlType = typeResolver.getType(sqlTypeCode, sqlTypeName);
                if (options.getRowKeyColumn().map(name -> name.equals(sqlColName)).orElse(false)) {
                    columnList.add(new RowKeyAdapter(sqlColName, sqlType, options));
                } else if (options.getAutoIncrementColumnName().map(name -> !name.equalsIgnoreCase(sqlColName)).orElse(true)) {
                    final C colKey = columnMap2.get(sqlColName);
                    final Class<?> dataType = frame.cols().type(colKey);
                    final DataFrameCursor<R,C> cursor = frame.cursor().atColKey(colKey);
                    final Function1<DataFrameValue<R,C>,?> mapper = options.getColumnMappings().getMapper(dataType);
                    columnList.add(new ValueAdapter(sqlColName, sqlType, cursor, mapper));
                }
            }
            return columnList;
        } catch (Exception ex) {
            throw new DataFrameException("Failed to resolve SQL column types for table " + tableName, ex);
        }
    }

    /**
     * Returns the SQL DDL statement to create a table for the DataFrame specified
     * @param frame     the frame instance
     * @param options   the sink options
     * @return          the create table statement
     */
    private String getCreateTableSql(DataFrame<R,C> frame, DbSinkOptions<R,C> options) {
        final SQLPlatform platform = options.getPlatform().orElseThrow(() -> new IllegalStateException("No SQL platform configured on options"));
        final StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ");
        ddl.append("\"");
        ddl.append(options.getTableName());
        ddl.append("\" (\n");

        options.getAutoIncrementColumnName().ifPresent(colName -> {
            ddl.append("    ");
            ddl.append("\"");
            ddl.append(colName);
            ddl.append("\" INTEGER");
            switch (platform) {
                case SQLITE:    ddl.append(" PRIMARY KEY");  break;
                case H2:        ddl.append(" AUTO_INCREMENT PRIMARY KEY");  break;
                case MYSQL:     ddl.append(" AUTO_INCREMENT PRIMARY KEY");  break;
                case HSQL:      ddl.append(" IDENTITY PRIMARY KEY");        break;
                case MSSQL:     ddl.append(" IDENTITY(1,1) PRIMARY KEY");   break;
                case GENERIC:   ddl.append(" IDENTITY PRIMARY KEY");        break;
                default:    throw new IllegalStateException("Unsupported SQL dialect: " + platform);
            }
            ddl.append(frame.cols().count() > 0 ? ",\n" : "");
        });

        options.getRowKeyColumn().ifPresent(colName -> {
            final Class<?> dataType = frame.rows().keyType();
            final Class<?> sqlType = options.getColumnMappings().getSqlType(dataType);
            final String typeInfo = getSqlTypeString(sqlType);
            ddl.append("    ");
            ddl.append("\"");
            ddl.append(colName);
            ddl.append("\" ").append(typeInfo);
            ddl.append(" NOT NULL");
            ddl.append(options.getAutoIncrementColumnName().isPresent() ? " PRIMARY KEY" : "");
            ddl.append(frame.cols().count() > 0 ? ",\n" : "");
        });

        frame.cols().forEach(column -> {
            final C key = column.key();
            final String colName = options.getColumnNames().apply(key);
            final boolean hasNull = column.hasNulls();
            final Class<?> dataType = frame.cols().type(key);
            final Class<?> sqlType = options.getColumnMappings().getSqlType(dataType);
            final String typeInfo = getSqlTypeString(sqlType);
            ddl.append("    ");
            ddl.append("\"");
            ddl.append(colName);
            ddl.append("\" ").append(typeInfo);
            ddl.append(hasNull ? " NULL" : " NOT NULL");
            ddl.append(",\n");
        });
        ddl.delete(ddl.length()-2, ddl.length());
        ddl.append("\n)");
        return ddl.toString();
    }


    /**
     * Returns the SQL type string for a CREATE TABLE statement
     * @param sqlClass  the SQL class
     * @return          the type string
     */
    private String getSqlTypeString(Class<?> sqlClass) {
        final SQLType sqlType = sqlTypeMap.get(sqlClass);
        if (sqlType == null) {
            throw new IllegalArgumentException("The SQL class is not a supported JDBC type: " + sqlClass);
        } else {
            switch (sqlType) {
                case BIT:       return "BIT";
                case BOOLEAN:   return "BIT";
                case TINYINT:   return "INTEGER";
                case SMALLINT:  return "INTEGER";
                case INTEGER:   return "INTEGER";
                case BIGINT:    return "BIGINT";
                case FLOAT:     return "DOUBLE";
                case DOUBLE:    return "DOUBLE";
                case DECIMAL:   return "DOUBLE";
                case VARCHAR:   return "VARCHAR(255)";
                case DATE:      return "DATE";
                case TIME:      return "TIME";
                case DATETIME:  return "DATETIME";
                default:    throw new IllegalStateException("Unsupported SQL type:" + sqlType);
            }
        }
    }



    /**
     * A convenience base class for building an adapter that maps DataFrame content to a SQL column of a well defined type
     */
    private abstract class ColumnAdapter {

        String colName;
        SQLType colType;

        /**
         * Constructor
         * @param colName   the SQL column name
         * @param colType   the SQL column type
         */
        ColumnAdapter(String colName, SQLType colType) {
            this.colName = colName;
            this.colType = colType;
        }

        @Override()
        public String toString() {
            return String.format("ColumnAdapter{type=%s, colName=%s}", colType, colName);
        }

        /**
         * Applies a parameter value to the SQL PreparedStatement
         * @param stmt          the PreparedStatement to apply parameter to
         * @param stmtIndex     the statement index for parameter
         * @param row           the row to extract a value from
         */
        abstract void apply(PreparedStatement stmt, int stmtIndex, DataFrameRow<R,C> row);
    }


    /**
     * A ColumnAdapter implementation that applies a row key from a DataFrameRow to the INSERT PreparedStatement
     */
    private class RowKeyAdapter extends ColumnAdapter {

        private SQLType rowKeyType;
        private Class<?> rowKeyClass;
        private Function1<R,?> rowKeyMapper;

        /**
         * Constructor
         * @param colName   the column name
         * @param colType   the column type
         * @param options   the sink options
         */
        RowKeyAdapter(String colName, SQLType colType, DbSinkOptions<R,C> options) {
            super(colName, colType);
            this.rowKeyMapper = options.getRowKeyMapper().orElseThrow(() -> new IllegalStateException("No mapper specified for row key mapping: " + colName));
            this.rowKeyClass = options.getRowKeySqlClass().orElseThrow(() -> new IllegalStateException("No SQL type specified for row key mapping: " + colName));
            this.rowKeyType = Optional.ofNullable(sqlTypeMap.get(rowKeyClass)).orElseThrow(() ->
                new IllegalArgumentException("The specified type is not a supported JDBC type: " + rowKeyClass)
            );
        }

        @Override
        void apply(PreparedStatement stmt, int stmtIndex, DataFrameRow<R, C> row) {
            final R rowKey = row.key();
            try {
                switch (rowKeyType) {
                    case BIT:       stmt.setBoolean(stmtIndex, rowKeyMapper.applyAsBoolean(rowKey));             break;
                    case BOOLEAN:   stmt.setBoolean(stmtIndex, rowKeyMapper.applyAsBoolean(rowKey));             break;
                    case TINYINT:   stmt.setInt(stmtIndex, rowKeyMapper.applyAsInt(rowKey));                     break;
                    case SMALLINT:  stmt.setInt(stmtIndex, rowKeyMapper.applyAsInt(rowKey));                     break;
                    case FLOAT:     stmt.setDouble(stmtIndex, rowKeyMapper.applyAsDouble(rowKey));               break;
                    case INTEGER:   stmt.setInt(stmtIndex, rowKeyMapper.applyAsInt(rowKey));                     break;
                    case BIGINT:    stmt.setLong(stmtIndex, rowKeyMapper.applyAsLong(rowKey));                   break;
                    case DOUBLE:    stmt.setDouble(stmtIndex, rowKeyMapper.applyAsDouble(rowKey));               break;
                    case DECIMAL:   stmt.setDouble(stmtIndex, rowKeyMapper.applyAsDouble(rowKey));               break;
                    case VARCHAR:   stmt.setString(stmtIndex, (String)rowKeyMapper.apply(rowKey));          break;
                    case DATE:      stmt.setDate(stmtIndex, (Date)rowKeyMapper.apply(rowKey));              break;
                    case TIME:      stmt.setTime(stmtIndex, (Time)rowKeyMapper.apply(rowKey));              break;
                    case DATETIME:  stmt.setTimestamp(stmtIndex, (Timestamp)rowKeyMapper.apply(rowKey));    break;
                    default:    throw new IllegalStateException("Unsupported column type:" + rowKeyType);
                }
            } catch (Exception ex) {
                throw new DataFrameException("Failed to apply row key to SQL statement at " + rowKey, ex);
            }
        }
    }


    /**
     * A ColumnAdapter implementation that applies a value extracted from a DataFrameRow to the INSERT PreparedStatement
     */
    private class ValueAdapter extends ColumnAdapter {

        private DataFrameCursor<R,C> cursor;
        private Function1<DataFrameValue<R,C>,?> mapper;

        /**
         * Constructor
         * @param colName   the column name in the database
         * @param colType   the column SQL type
         * @param cursor    the frame cursor, initialized to the correct column ordinal
         * @param mapper    the mapper to apply DataFrameValue to appropriate SQL type
         */
        ValueAdapter(String colName, SQLType colType, DataFrameCursor<R,C> cursor, Function1<DataFrameValue<R,C>,?> mapper) {
            super(colName, colType);
            this.cursor = cursor;
            this.mapper = mapper;
        }

        @Override()
        void apply(PreparedStatement stmt, int stmtIndex, DataFrameRow<R,C> row) {
            try {
                this.cursor.atRowOrdinal(row.ordinal());
                if (cursor.isNull()) {
                    stmt.setNull(stmtIndex, colType.getTypeCode());
                } else {
                    switch (colType) {
                        case BIT:       stmt.setBoolean(stmtIndex, mapper.applyAsBoolean(cursor));          break;
                        case BOOLEAN:   stmt.setBoolean(stmtIndex, mapper.applyAsBoolean(cursor));          break;
                        case TINYINT:   stmt.setInt(stmtIndex, mapper.applyAsInt(cursor));                  break;
                        case SMALLINT:  stmt.setInt(stmtIndex, mapper.applyAsInt(cursor));                  break;
                        case FLOAT:     stmt.setDouble(stmtIndex, mapper.applyAsDouble(cursor));            break;
                        case INTEGER:   stmt.setInt(stmtIndex, mapper.applyAsInt(cursor));                  break;
                        case BIGINT:    stmt.setLong(stmtIndex, mapper.applyAsLong(cursor));                break;
                        case DOUBLE:    stmt.setDouble(stmtIndex, mapper.applyAsDouble(cursor));            break;
                        case DECIMAL:   stmt.setDouble(stmtIndex, mapper.applyAsDouble(cursor));            break;
                        case VARCHAR:   stmt.setString(stmtIndex, (String)mapper.apply(cursor));            break;
                        case DATE:      stmt.setDate(stmtIndex, (Date)mapper.apply(cursor));                break;
                        case TIME:      stmt.setTime(stmtIndex, (Time)mapper.apply(cursor));                break;
                        case DATETIME:  stmt.setTimestamp(stmtIndex, (Timestamp)mapper.apply(cursor));      break;
                        default:    throw new IllegalStateException("Unsupported column type:" + colType);
                    }
                }
            } catch (Exception ex) {
                final String coordinates = String.format("(%s, %s)", cursor.rowKey(), cursor.colKey());
                throw new DataFrameException("Failed to apply value to SQL statement at " + coordinates, ex);
            }
        }
    }
}
