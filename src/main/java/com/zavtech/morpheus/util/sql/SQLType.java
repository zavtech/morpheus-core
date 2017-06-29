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

import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

/**
 * An enum that defines commonly used SQL data types
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public enum SQLType {

    BIT(Types.BIT, Boolean.class),
    BOOLEAN(Types.BOOLEAN, Boolean.class),
    INTEGER(Types.INTEGER, Integer.class),
    TINYINT(Types.TINYINT, Integer.class),
    SMALLINT(Types.SMALLINT, Integer.class),
    BIGINT(Types.BIGINT, Long.class),
    DOUBLE(Types.DOUBLE, Double.class),
    NUMERIC(Types.NUMERIC, Double.class),
    DECIMAL(Types.DECIMAL, Double.class),
    FLOAT(Types.FLOAT, Double.class),
    REAL(Types.REAL, Double.class),
    NVARCHAR(Types.NVARCHAR, String.class),
    CHAR(Types.CHAR, String.class),
    VARCHAR(Types.VARCHAR, String.class),
    CLOB(Types.CLOB, String.class),
    DATE(Types.DATE, LocalDate.class),
    TIME(Types.TIME, LocalTime.class),
    DATETIME(Types.TIMESTAMP, LocalDateTime.class);

    private int typeCode;
    private Class<?> typeClass;

    /**
     * Constructor
     * @param typeClass the type class
     */
    SQLType(int typeCode, Class<?> typeClass) {
        this.typeCode = typeCode;
        this.typeClass = typeClass;
    }

    /**
     * Returns the class this type maps to
     * @return  the class this type maps to
     */
    public Class<?> typeClass() {
        return typeClass;
    }

    /**
     * Returns the SQL type code for this type
     * @return  the SQL type code
     * @see Types
     */
    public int getTypeCode() {
        return typeCode;
    }


    /**
     * Returns the type resolver for the platform specified
     * @param platform  the SQL platform code
     * @return          the type resolver
     */
    public static TypeResolver getTypeResolver(SQLPlatform platform) {
        switch (platform) {
            case SQLITE:    return new SqliteTypeResolver();
            default:        return new DefaultTypeResolver();
        }
    }



    /**
     * An interface to a tyoe resolver given a sql code and tyoe name
     */
    public interface TypeResolver {

        /**
         * Returns the type given the JDBC type code and type name
         * @param sqlCode       the jdbc type code from java.sql.Types
         * @param typeName      the type name
         * @return              the matching Type
         */
        SQLType getType(int sqlCode, String typeName);
    }


    /**
     * The default TypeResolver implementation
     */
    private static class DefaultTypeResolver implements TypeResolver {
        @Override
        public SQLType getType(int sqlCode, String typeName) {
            switch (sqlCode) {
                case Types.BIT:         return SQLType.BIT;
                case Types.BOOLEAN:     return SQLType.BOOLEAN;
                case Types.INTEGER:     return SQLType.INTEGER;
                case Types.TINYINT:     return SQLType.TINYINT;
                case Types.SMALLINT:    return SQLType.SMALLINT;
                case Types.BIGINT:      return SQLType.BIGINT;
                case Types.DOUBLE:      return SQLType.DOUBLE;
                case Types.NUMERIC:     return SQLType.NUMERIC;
                case Types.DECIMAL:     return SQLType.DECIMAL;
                case Types.FLOAT:       return SQLType.FLOAT;
                case Types.REAL:        return SQLType.REAL;
                case Types.NVARCHAR:    return SQLType.NVARCHAR;
                case Types.CHAR:        return SQLType.CHAR;
                case Types.VARCHAR:     return SQLType.VARCHAR;
                case Types.CLOB:        return SQLType.CLOB;
                case Types.DATE:        return SQLType.DATE;
                case Types.TIME:        return SQLType.TIME;
                case Types.TIMESTAMP:   return SQLType.DATETIME;
                default:
                    throw new RuntimeException("Unsupported data type for " + typeName + ", sqlType: " + sqlCode);
            }
        }
    }



    /**
     * A SQLITE specific TypeResolver implementation that deals with type affinity issues
     */
    private static class SqliteTypeResolver implements TypeResolver {

        private static final Map<String,SQLType> typeMap = new HashMap<>();
        private static TypeResolver defaultResolver = new DefaultTypeResolver();

        /**
         * Static initializer
         */
        static {
            typeMap.put("BIT", SQLType.BIT);
            typeMap.put("BOOLEAN", SQLType.BOOLEAN);
            typeMap.put("TINYINT", SQLType.TINYINT);
            typeMap.put("SMALLINT", SQLType.SMALLINT);
            typeMap.put("INTEGER", SQLType.INTEGER);
            typeMap.put("BIGINT", SQLType.BIGINT);
            typeMap.put("FLOAT", SQLType.FLOAT);
            typeMap.put("REAL", SQLType.REAL);
            typeMap.put("NUMERIC", SQLType.NUMERIC);
            typeMap.put("DOUBLE", SQLType.DOUBLE);
            typeMap.put("DECIMAL", SQLType.DECIMAL);
            typeMap.put("CHAR", SQLType.CHAR);
            typeMap.put("VARCHAR", SQLType.VARCHAR);
            typeMap.put("DATE", SQLType.DATE);
            typeMap.put("TIME", SQLType.TIME);
            typeMap.put("DATETIME", SQLType.DATETIME);
            typeMap.put("TIMESTAMP", SQLType.DATETIME);
        }


        @Override
        public SQLType getType(int sqlCode, String typeName) {
            for (String token : typeName.toUpperCase().split("\\s+")) {
                final SQLType type = typeMap.get(token);
                if (type != null) {
                    return type;
                }
            }
            return defaultResolver.getType(sqlCode, typeName);
        }
    }

}
