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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A class used to extract values from a JDBC ResultSet
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class SQLExtractor implements Cloneable {

    private static boolean initialized = false;
    private static final Map<Class<?>,SQLExtractor> extractorMap = new HashMap<>();

    private Class<?> dataType;
    private SQLPlatform platform;

    /**
     * Function that can be bound to an extractor
     */
    @FunctionalInterface
    public interface Function<T> {

        /**
         * Returns a value for an entry in the result set
         * @param rs    the result set reference
         * @param colIndex  the column index to extract value from
         * @return          the resulting value
         */
        T apply(ResultSet rs, int colIndex) throws SQLException;
    }

    /**
     * Constructor
     * @param dataType  the data type for extractor
     */
    private SQLExtractor(Class<?> dataType) {
        this.dataType = dataType;
        this.platform = SQLPlatform.GENERIC;
    }

    /**
     * Initializes the default set of extractors
     * @return      the defailt set of extractors
     */
    private static Map<Class<?>,SQLExtractor> extractors() {
        if (!initialized) {
            extractorMap.put(boolean.class, new BooleanExtractor());
            extractorMap.put(int.class, new IntegerExtractor());
            extractorMap.put(long.class, new LongExtractor());
            extractorMap.put(double.class, new DoubleExtractor());
            extractorMap.put(Boolean.class, new BooleanExtractor());
            extractorMap.put(Integer.class, new IntegerExtractor());
            extractorMap.put(Long.class, new LongExtractor());
            extractorMap.put(Double.class, new DoubleExtractor());
            extractorMap.put(String.class, new StringExtractor());
            extractorMap.put(java.util.Date.class, new DateExtractor());
            extractorMap.put(java.sql.Date.class, new DateExtractor());
            extractorMap.put(java.sql.Time.class, new TimeExtractor());
            extractorMap.put(Timestamp.class, new TimestampExtractor());
            extractorMap.put(LocalDate.class, new LocalDateExtractor());
            extractorMap.put(LocalTime.class, new LocalTimeExtractor());
            extractorMap.put(LocalDateTime.class, new LocalDateTimeExtractor());
            initialized = true;
        }
        return extractorMap;
    }


    /**
     * Registers a database value extractor for the type specified
     * @param dataType      the data type
     * @param extractor     the extractor reference
     */
    public static void register(Class<?> dataType, SQLExtractor extractor) {
        Objects.requireNonNull(dataType, "The data type cannot be null");
        Objects.requireNonNull(extractor, "The database extractor cannot be null");
        extractorMap.put(dataType, extractor);
    }


    /**
     * Returns an extractor for the data type specified
     * @param dataType  the data type to extractor from a SQL ResultSet
     * @param platform  the sql platform
     * @return          the extractor for the type argument
     */
    public static SQLExtractor with(Class<?> dataType, SQLPlatform platform) {
        return extractors().getOrDefault(dataType, new ObjectExtractor(dataType)).with(platform);
    }

    /**
     * Returns a newly created extractor for the type using the handler specified
     * @param dataType  the data type produced by this extractor
     * @param function  the extraction function
     * @return          the newly created extractor
     */
    public static <T> SQLExtractor with(Class<T> dataType, Function<T> function) {
        return new SQLExtractor(dataType) {
            @Override
            @SuppressWarnings("unchecked")
            public <V> V getValue(ResultSet rs, int colIndex) throws SQLException {
                return (V)function.apply(rs, colIndex);
            }
        };
    }

    /**
     * Returns a copy of this extractor with the platform set
     * @param platform  the database platform
     * @return          the copy of the extractor
     */
    public SQLExtractor with(SQLPlatform platform) {
        try {
            final SQLExtractor clone = (SQLExtractor)super.clone();
            clone.dataType = dataType;
            clone.platform = platform;
            return clone;
        } catch (Exception ex) {
            throw new RuntimeException("Failed to clone DbExtractor", ex);
        }
    }

    /**
     * Returns the data type for this extractor
     * @return  the data type for extractor
     */
    public Class<?> getDataType() {
        return dataType;
    }

    /**
     * Returns the platform for this extractor
     * @return      the platform
     */
    public SQLPlatform getPlatform() {
        return platform;
    }

    /**
     * Returns the value from the result set at current row and col index specified
     * @param rs        the result set reference
     * @param colIndex  the result set column index
     * @return          the value extracted from result set
     * @throws SQLException
     */
    public boolean getBoolean(ResultSet rs, int colIndex) throws SQLException {
        return rs.getBoolean(colIndex);
    }

    /**
     * Returns the value from the result set at current row and col index specified
     * @param rs        the result set reference
     * @param colIndex  the result set column index
     * @return          the value extracted from result set
     * @throws SQLException
     */
    public int getInt(ResultSet rs, int colIndex) throws SQLException {
        return rs.getInt(colIndex);
    }

    /**
     * Returns the value from the result set at current row and col index specified
     * @param rs        the result set reference
     * @param colIndex  the result set column index
     * @return          the value extracted from result set
     * @throws SQLException
     */
    public long getLong(ResultSet rs, int colIndex) throws SQLException {
        return rs.getLong(colIndex);
    }

    /**
     * Returns the value from the result set at current row and col index specified
     * @param rs        the result set reference
     * @param colIndex  the result set column index
     * @return          the value extracted from result set
     * @throws SQLException
     */
    public double getDouble(ResultSet rs, int colIndex) throws SQLException {
        final double value = rs.getDouble(colIndex);
        return rs.wasNull() ? Double.NaN : value;
    }

    /**
     * Returns the value from the result set at current row and col index specified
     * @param rs        the result set reference
     * @param colIndex  the result set column index
     * @return          the value extracted from result set
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    public <V> V getValue(ResultSet rs, int colIndex) throws SQLException {
        return (V)rs.getObject(colIndex);
    }



    private static class BooleanExtractor extends SQLExtractor {

        /**
         * Constructor
         */
        BooleanExtractor() {
            super(Boolean.class);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <V> V getValue(ResultSet rs, int colIndex) throws SQLException {
            final boolean value = rs.getBoolean(colIndex);
            return rs.wasNull() ? null : value ? (V)Boolean.TRUE : (V)Boolean.FALSE;
        }
    }


    private static class DoubleExtractor extends SQLExtractor {

        /**
         * Constructor
         */
        DoubleExtractor() {
            super(Double.class);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <V> V getValue(ResultSet rs, int colIndex) throws SQLException {
            final double value = rs.getDouble(colIndex);
            return rs.wasNull() ? null : (V)new Double(value);
        }
    }


    private static class IntegerExtractor extends SQLExtractor {

        /**
         * Constructor
         */
        IntegerExtractor() {
            super(Integer.class);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <V> V getValue(ResultSet rs, int colIndex) throws SQLException {
            final int value = rs.getInt(colIndex);
            return rs.wasNull() ? null : (V)new Integer(value);
        }
    }


    private static class LongExtractor extends SQLExtractor {

        /**
         * Constructor
         */
        LongExtractor() {
            super(Long.class);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <V> V getValue(ResultSet rs, int colIndex) throws SQLException {
            final long value = rs.getLong(colIndex);
            return rs.wasNull() ? null : (V)new Long(value);
        }
    }


    private static class DateExtractor extends SQLExtractor {

        /**
         * Constructor
         */
        DateExtractor() {
            super(Date.class);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <V> V getValue(ResultSet rs, int colIndex) throws SQLException {
            return (V)rs.getDate(colIndex);
        }
    }


    private static class TimeExtractor extends SQLExtractor {

        /**
         * Constructor
         */
        TimeExtractor() {
            super(Time.class);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <V> V getValue(ResultSet rs, int colIndex) throws SQLException {
            return (V)rs.getTime(colIndex);
        }
    }


    private static class TimestampExtractor extends SQLExtractor {

        /**
         * Constructor
         */
        TimestampExtractor() {
            super(Timestamp.class);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <V> V getValue(ResultSet rs, int colIndex) throws SQLException {
            return (V)rs.getTimestamp(colIndex);
        }
    }


    private static class LocalDateExtractor extends SQLExtractor {

        private ZoneId GMT = ZoneId.of("GMT");

        /**
         * Constructor
         */
        LocalDateExtractor() {
            super(LocalDate.class);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <V> V getValue(ResultSet rs, int colIndex) throws SQLException {
            if (getPlatform() == SQLPlatform.SQLITE) {
                final String value = rs.getString(colIndex);
                if (value == null) {
                    return null;
                } else if (value.matches("\\d+")) {
                    return (V)Instant.ofEpochMilli(Long.parseLong(value)).atZone(GMT).toLocalDate();
                } else {
                    return (V)LocalDate.parse(value);
                }
            } else {
                final java.sql.Date date = rs.getDate(colIndex);
                return date != null ? (V)date.toLocalDate() : null;
            }
        }
    }


    private static class LocalTimeExtractor extends SQLExtractor {

        /**
         * Constructor
         */
        LocalTimeExtractor() {
            super(LocalTime.class);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <V> V getValue(ResultSet rs, int colIndex) throws SQLException {
            final Time time = rs.getTime(colIndex);
            return time != null ? (V)time.toLocalTime() : null;
        }
    }


    private static class LocalDateTimeExtractor extends SQLExtractor {

        /**
         * Constructor
         */
        LocalDateTimeExtractor() {
            super(LocalDateTime.class);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <V> V getValue(ResultSet rs, int colIndex) throws SQLException {
            final Timestamp timestamp = rs.getTimestamp(colIndex);
            return timestamp != null ? (V)timestamp.toLocalDateTime() : null;
        }
    }


    private static class StringExtractor extends SQLExtractor {

        /**
         * Constructor
         */
        StringExtractor() {
            super(String.class);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <V> V getValue(ResultSet rs, int colIndex) throws SQLException {
            return (V)rs.getString(colIndex);
        }
    }


    private static class ObjectExtractor extends SQLExtractor {

        /**
         * Constructor
         * @param dataType  the data type for extractor
         */
        ObjectExtractor(Class<?> dataType) {
            super(dataType);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <V> V getValue(ResultSet rs, int colIndex) throws SQLException {
            return (V)rs.getObject(colIndex);
        }
    }


}
