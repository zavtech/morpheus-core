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
package com.zavtech.morpheus.util.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.function.Function;

/**
 * A class used to capture a SQL expression and associated arguments, with convenience execute() functions.
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public class SQL implements java.io.Serializable {

    public static final long serialVersionUID = 1L;

    private String expression;
    private Object[] args;

    /**
     * Constructor
     * @param expression    the sql expression
     * @param args          the sql expression arguments if parameterized
     */
    public SQL(String expression, Object... args) {
        this.expression = expression;
        this.args = args;
    }

    /**
     * Returns a newly created SQL query from the args
     * @param expression    the SQL expression, which can be parameterized with ?
     * @param args          the SQL expression arguments if paramererized
     * @return              the newly created SQL object
     */
    public static SQL of(String expression, Object... args) {
        return new SQL(expression, args);
    }


    /**
     * Executes this SQL query using the database connection provided
     * @param conn      the database connection
     * @param handler   the ResultSet handler
     * @param <T>       the type of the return Object
     * @return          the resulting object
     * @throws SQLException if the query fails
     */
    public <T> T executeQuery(Connection conn, Function<ResultSet,T> handler) throws SQLException {
        try {
            if (args != null && args.length > 0) {
                try (PreparedStatement stmt = conn.prepareStatement(expression)) {
                    final ResultSet resultSet = bindArgs(stmt).executeQuery();
                    return handler.apply(resultSet);
                }
            } else {
                try (Statement stmt = conn.createStatement();) {
                    final ResultSet resultSet = stmt.executeQuery(expression);
                    return handler.apply(resultSet);
                }
            }
        } finally {
            close(conn);
        }
     }


    /**
     * Binds arguments to prepared statement
     * @param stmt  the prepared statement reference
     * @return      the same as arg
     * @throws SQLException if binding fails
     */
    private PreparedStatement bindArgs(PreparedStatement stmt) throws SQLException {
        for (int i=0; i<args.length; ++i) {
            final Object value = args[i];
            if      (value instanceof Boolean)          stmt.setBoolean(i+1, (Boolean)value);
            else if (value instanceof Short)            stmt.setShort(i+1, (Short)value);
            else if (value instanceof Integer)          stmt.setInt(i+1, (Integer)value);
            else if (value instanceof Float)            stmt.setFloat(i+1, (Float)value);
            else if (value instanceof Long)             stmt.setLong(i+1, (Long)value);
            else if (value instanceof Double)           stmt.setDouble(i+1, (Double)value);
            else if (value instanceof String)           stmt.setString(i+1, (String)value);
            else if (value instanceof java.sql.Date)    stmt.setDate(i+1, (java.sql.Date)value);
            else if (value instanceof Timestamp)        stmt.setTimestamp(i+1, (Timestamp)value);
            else if (value instanceof LocalDate)        stmt.setDate(i + 1, java.sql.Date.valueOf((LocalDate)value));
            else if (value instanceof LocalTime)        stmt.setTime(i+1, Time.valueOf((LocalTime)value));
            else if (value instanceof LocalDateTime)    stmt.setTimestamp(i+1, Timestamp.valueOf((LocalDateTime)value));
            else if (value instanceof ZonedDateTime) {
                final ZonedDateTime zonedDateTime = (ZonedDateTime)value;
                final LocalDateTime dateTime = zonedDateTime.toLocalDateTime();
                stmt.setTimestamp(i+1, Timestamp.valueOf(dateTime));
            } else {
                throw new RuntimeException("Unsupported argument, cannot be bound to SQL statement: " + value);
            }
        }
        return stmt;
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

}
