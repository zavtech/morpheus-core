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
package com.zavtech.morpheus.util.text.parser;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import com.zavtech.morpheus.util.Asserts;
import com.zavtech.morpheus.util.functions.Function1;
import com.zavtech.morpheus.util.functions.FunctionStyle;
import com.zavtech.morpheus.util.functions.ToBooleanFunction;

/**
 * A Function implementation of a Parser than can parse a String value into some another type.
 *
 * A design feature of the Parser framework is that it can be used to parse primitives while avoiding
 * boxing, which can make a difference when parsing very large files. The Function1 interface exposes
 * a number of applyXXX() methods that yield primitives, and a getStyle() method that can be queried
 * to check what type the function supports.
 *
 * @param <T>   the type produced by this parser
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public abstract class Parser<T> extends Function1<String,T> {

    private static final Set<String> defaultNullSet = new HashSet<>(Arrays.asList("null", "NULL", "Null", "N/A", "n/a", "-"));
    private static final ToBooleanFunction<String> defaultNullCheck = value -> value == null || value.trim().length() == 0 || defaultNullSet.contains(value);

    private Class<T> type;
    private ToBooleanFunction<String> nullChecker;

    /**
     * Constructor
     * @param style         the style for this function
     * @param type          the data type produced by this parser
     * @param nullChecker   the null checker function
     */
    public Parser(FunctionStyle style, Class<T> type, ToBooleanFunction<String> nullChecker) {
        super(style);
        this.type = type;
        this.nullChecker = nullChecker;
    }

    /**
     * Returns the data type produced by this parser
     * @return  the data type for this parser
     */
    public final Class<T> getType() {
        return type;
    }

    /**
     * Returns the null checker for this parser
     * @return      the null checker
     */
    public final ToBooleanFunction<String> getNullChecker() {
        return nullChecker;
    }

    /**
     * Applies the null checker function for this Parser
     * @param nullChecker   the null checker function
     * @return              this Parser
     */
    public Parser<T> withNullChecker(ToBooleanFunction<String> nullChecker) {
        Asserts.notNull(nullChecker, "The null checker cannot be null");
        this.nullChecker = nullChecker;
        return this;
    }

    /**
     * Returns true if this parser can process the value specified
     * @param value     the value to check if can be parsed by this parser
     * @return          true if the value can be parsed
     */
    public abstract boolean isSupported(String value);


    /**
     * Creates an BOOLEAN parser that wraps to function provided
     * @param function  the function to wrap
     * @return          the newly created parser
     */
    public static Parser<Boolean> forBoolean(ToBooleanFunction<String> function) {
        return new Parser<Boolean>(FunctionStyle.BOOLEAN, Boolean.class, v -> v == null) {
            @Override
            public final boolean applyAsBoolean(String value) {
                return value != null && function.applyAsBoolean(value);
            }
            @Override
            public final boolean isSupported(String value) {
                return false;
            }
        };
    }

    /**
     * Creates an INTEGER parser that wraps to function provided
     * @param function  the function to wrap
     * @return          the newly created parser
     */
    public static Parser<Integer> forInt(ToIntFunction<String> function) {
        return new Parser<Integer>(FunctionStyle.INTEGER, Integer.class, v -> v == null) {
            @Override
            public final int applyAsInt(String value) {
                return value != null ? function.applyAsInt(value) : 0;
            }
            @Override
            public final boolean isSupported(String value) {
                return false;
            }
        };
    }

    /**
     * Creates an LONG parser that wraps to function provided
     * @param function  the function to wrap
     * @return          the newly created parser
     */
    public static Parser<Long> forLong(ToLongFunction<String> function) {
        return new Parser<Long>(FunctionStyle.LONG, Long.class, v -> v == null) {
            @Override
            public final long applyAsLong(String value) {
                return value != null ? function.applyAsLong(value) : 0L;
            }
            @Override
            public final boolean isSupported(String value) {
                return false;
            }
        };
    }

    /**
     * Creates an DOUBLE parser that wraps to function provided
     * @param function  the function to wrap
     * @return          the newly created parser
     */
    public static Parser<Double> forDouble(ToDoubleFunction<String> function) {
        return new Parser<Double>(FunctionStyle.DOUBLE, Double.class, v -> v == null) {
            @Override
            public final double applyAsDouble(String value) {
                return value != null ? function.applyAsDouble(value) : Double.NaN;
            }
            @Override
            public final boolean isSupported(String value) {
                return false;
            }
        };
    }

    /**
     * Creates an OBJECT parser that wraps to function provided
     * @param function  the function to wrap
     * @param type      the type for this parser
     * @param <O>       the output type
     * @return          the newly created parser
     */
    public static <O> Parser<O> forObject(Class<O> type, Function<String,O> function) {
        return new Parser<O>(FunctionStyle.OBJECT, type, v -> v == null) {
            @Override
            public final O apply(String value) {
                return value != null ? function.apply(value) : null;
            }
            @Override
            public final boolean isSupported(String value) {
                return false;
            }
        };
    }

    /**
     * Returns a newly created Parser for Boolean
     * @return  newly created Parser
     */
    public static Parser<Boolean> ofBoolean() {
        return new ParserOfBoolean(defaultNullCheck);
    }

    /**
     * Returns a newly created Parser for Integer
     * @return  newly created Parser
     */
    public static Parser<Integer> ofInteger() {
        return new ParserOfInteger(defaultNullCheck);
    }

    /**
     * Returns a newly created Parser for Long
     * @return  newly created Parser
     */
    public static Parser<Long> ofLong() {
        return new ParserOfLong(defaultNullCheck);
    }

    /**
     * Returns a newly created Parser for Double
     * @return  newly created Parser
     */
    public static Parser<Double> ofDouble(String pattern, int multipler) {
        final DecimalFormat decimalFormat = createDecimalFormat(pattern, multipler);
        return new ParserOfDouble(defaultNullCheck, () -> decimalFormat);
    }

    /**
     * Returns a newly created Parser for BigDecimal
     * @return  newly created Parser
     */
    public static Parser<BigDecimal> ofBigDecimal() {
        return new ParserOfBigDecimal(defaultNullCheck);
    }

    /**
     * Returns a newly created Parser for an Enum class
     * @param enumClass the enum class
     * @return  newly created Parser
     */
    public static <T extends Enum> Parser<T> ofEnum(Class<T> enumClass) {
        return new ParserOfEnum<>(enumClass, defaultNullCheck);
    }

    /**
     * Returns a newly created Parser for String
     * @return  newly created Parser
     */
    public static Parser<String> ofString() {
        return new ParserOfString(defaultNullCheck);
    }

    /**
     * Returns a newly created Parser for java.util.Date
     * @return  newly created Parser
     */
    public static Parser<Date> ofDate() {
        return new ParserOfDate<>(Date.class, defaultNullCheck);
    }

    /**
     * Returns a newly created Parser for LocalTime
     * @return  newly created Parser
     */
    public static Parser<LocalTime> ofLocalTime(String pattern) {
        return ofLocalTime(DateTimeFormatter.ofPattern(pattern));
    }

    /**
     * Returns a newly created Parser for LocalTime
     * @param format    the date format
     * @return  newly created Parser
     */
    public static Parser<LocalTime> ofLocalTime(DateTimeFormatter format) {
        return new ParserOfLocalTime(defaultNullCheck, () -> format);
    }

    /**
     * Returns a newly created Parser for LocalDate
     * @param pattern   the date time format pattern
     * @return  newly created Parser
     */
    public static Parser<LocalDate> ofLocalDate(String pattern) {
        return ofLocalDate(DateTimeFormatter.ofPattern(pattern));
    }

    /**
     * Returns a newly created Parser for LocalDate
     * @param format    the date format
     * @return  newly created Parser
     */
    public static Parser<LocalDate> ofLocalDate(DateTimeFormatter format) {
        return new ParserOfLocalDate(defaultNullCheck, () -> format);
    }

    /**
     * Returns a newly created Parser for LocalDateTime
     * @param pattern   the date time format pattern
     * @return  newly created Parser
     */
    public static Parser<LocalDateTime> ofLocalDateTime(String pattern) {
        return ofLocalDateTime(DateTimeFormatter.ofPattern(pattern));
    }

    /**
     * Returns a newly created Parser for LocalDateTime
     * @param format    the date time format
     * @return  newly created Parser
     */
    public static Parser<LocalDateTime> ofLocalDateTime(DateTimeFormatter format) {
        return new ParserOfLocalDateTime(defaultNullCheck, () -> format);
    }

    /**
     * Returns a newly created Parser for ZoneDateTime
     * @param pattern   the date time format pattern
     * @param zoneId    the zone id
     * @return  newly created Parser
     */
    public static Parser<ZonedDateTime> ofZonedDateTime(String pattern, ZoneId zoneId) {
        return ofZonedDateTime(DateTimeFormatter.ofPattern(pattern).withZone(zoneId));
    }

    /**
     * Returns a newly created Parser for ZoneDateTime
     * @param format    the date time format
     * @return  newly created Parser
     */
    public static Parser<ZonedDateTime> ofZonedDateTime(DateTimeFormatter format) {
        return new ParserOfZonedDateTime(defaultNullCheck, () -> format);
    }

    /**
     * Returns a newly created Parser for Period
     * @return  newly created Parser
     */
    public static Parser<Period> ofPeriod() {
        return new ParserOfPeriod(defaultNullCheck);
    }

    /**
     * Returns a newly created Parser for TimeZone
     * @return  newly created Parser
     */
    public static Parser<TimeZone> ofTimeZone() {
        return new ParserOfTimeZone(defaultNullCheck);
    }

    /**
     * Returns a newly created Parser for ZoneId
     * @return  newly created Parser
     */
    public static Parser<ZoneId> ofZoneId() {
        return new ParserOfZoneId(defaultNullCheck);
    }

    /**
     * Returns a newly created Parser for Object
     * @return  newly created Parser
     */
    public static Parser<Object> ofObject() {
        return new ParserOfObject(defaultNullCheck);
    }

    /**
     * Returns a newly created DecimalFormat object
     * @param pattern       the format pattern
     * @param multiplier    the multiplier
     * @return              the formatter
     */
    private static DecimalFormat createDecimalFormat(String pattern, int multiplier) {
        final DecimalFormat decimalFormat = new DecimalFormat(pattern);
        decimalFormat.setMultiplier(multiplier);
        return decimalFormat;
    }

}
