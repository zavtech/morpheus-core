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
package com.zavtech.morpheus.array;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Currency;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * Defines the universe of array types currently supported by the Morpheus Analytics library.
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public enum ArrayType {

    BOOLEAN,
    INTEGER,
    LONG,
    DOUBLE,
    DATE,
    STRING,
    ENUM,
    OBJECT,
    YEAR,
    CURRENCY,
    ZONE_ID,
    TIME_ZONE,
    INSTANT,
    LOCAL_DATE,
    LOCAL_TIME,
    LOCAL_DATETIME,
    ZONED_DATETIME;

    private static final Integer DEFAULT_INT = 0;
    private static final Long DEFAULT_LONG = 0L;
    private static final Double DEFAULT_DOUBLE = Double.NaN;
    private static final Map<Class<?>,ArrayType> typeMap = new HashMap<>();

    /**
     * Static initializer
     */
    static {
        try {
            typeMap.put(boolean.class, BOOLEAN);
            typeMap.put(int.class, INTEGER);
            typeMap.put(long.class, LONG);
            typeMap.put(double.class, DOUBLE);
            typeMap.put(Boolean.class, BOOLEAN);
            typeMap.put(Integer.class, INTEGER);
            typeMap.put(Long.class, LONG);
            typeMap.put(Double.class, DOUBLE);
            typeMap.put(Date.class, DATE);
            typeMap.put(Instant.class, INSTANT);
            typeMap.put(String.class, STRING);
            typeMap.put(Object.class, OBJECT);
            typeMap.put(Year.class, YEAR);
            typeMap.put(Currency.class, CURRENCY);
            typeMap.put(ZoneId.class, ZONE_ID);
            typeMap.put(TimeZone.class, TIME_ZONE);
            typeMap.put(LocalDate.class, LOCAL_DATE);
            typeMap.put(LocalTime.class, LOCAL_TIME);
            typeMap.put(LocalDateTime.class, LOCAL_DATETIME);
            typeMap.put(ZonedDateTime.class, ZONED_DATETIME);
            typeMap.put(sun.util.calendar.ZoneInfo.class, TIME_ZONE);
            typeMap.put(Class.forName("java.time.ZoneRegion"), ZONE_ID);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Returns the default value for this type, can be null
     * @param <T>   the array element type
     * @return      the default value, which can be null
     */
    @SuppressWarnings("unchecked")
    public static <T> T defaultValue(Class<T> type) {
        switch (ArrayType.of(type)) {
            case BOOLEAN:           return (T)Boolean.FALSE;
            case INTEGER:           return (T)DEFAULT_INT;
            case LONG:              return (T)DEFAULT_LONG;
            case DOUBLE:            return (T)DEFAULT_DOUBLE;
            default:                return null;
        }
    }

    /**
     * Returns the ArrayType for the type class specified
     * @param typeClass the type class
     * @return          the corresponding array type
     */
    public static ArrayType of(Class<?> typeClass) {
        return typeClass.isEnum() ? ENUM : typeMap.getOrDefault(typeClass, OBJECT);
    }

    /**
     * Returns true if this represents a numeric type
     * @return      true if numeric type
     */
    public boolean isNumeric() {
        switch (this) {
            case INTEGER:   return true;
            case LONG:      return true;
            case DOUBLE:    return true;
            default:        return false;
        }
    }

    /**
     * Retruns true if this is a BOOLEAN
     * @return  true if BOOLEAN
     */
    public boolean isBoolean() {
        return this == BOOLEAN;
    }

    /**
     * Returns true if this is an INTEGER
     * @return  true if INTEGER
     */
    public boolean isInteger() {
        return this == INTEGER;
    }

    /**
     * Returns true if this is a LONG
     * @return  true if LONG
     */
    public boolean isLong() {
        return this == LONG;
    }

    /**
     * Returns true if this is a DOUBLE
     * @return  true if DOUBLE
     */
    public boolean isDouble() {
        return this == DOUBLE;
    }

    /**
     * Returns true if this is a STRING
     * @return  true if STRING
     */
    public boolean isString() {
        return this == STRING;
    }

    /**
     * Returns true if this is a OBJECT
     * @return  true if OBJECT
     */
    public boolean isObject() {
        return this == OBJECT;
    }


}
