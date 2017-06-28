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
package com.zavtech.morpheus.util.text.printer;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.TimeZone;
import java.util.function.DoubleFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.LongFunction;
import java.util.function.Supplier;

import com.zavtech.morpheus.util.functions.BooleanFunction;
import com.zavtech.morpheus.util.functions.Function2;
import com.zavtech.morpheus.util.functions.FunctionStyle;

/**
 * A Function implementation that defines a Printer than can convert a value into a well formatted String value
 *
 * @param <T>   the type produced by this parser
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public abstract class Printer<T> extends Function2<T,String> {

    static final Supplier<String> DEFAULT_NULL = () -> "null";

    private Supplier<String> nullValue;


    /**
     * Constructor
     * @param style     the style for this printer
     * @param nullValue the null value supplier
     */
    public Printer(FunctionStyle style, Supplier<String> nullValue) {
        super(style);
        this.nullValue = nullValue;
    }

    /**
     * Returns the null supplier for this Printer
     * @return      the null supplier
     */
    public final Supplier<String> getNullValue() {
        return nullValue;
    }

    /**
     * Sets the null value supplier for this Printer
     * @param nullValue the null value supplier
     * @return          this Printer
     */
    public Printer<T> withNullValue(Supplier<String> nullValue) {
        this.nullValue = nullValue;
        return this;
    }

    /**
     * Creates an BOOLEAN Printer that wraps the function provided
     * @param function  the function to wrap
     * @return          the newly created function Printer
     */
    public static Printer<Boolean> forBoolean(BooleanFunction<String> function) {
        return new Printer<Boolean>(FunctionStyle.BOOLEAN, DEFAULT_NULL) {
            @Override
            public final String apply(boolean input) {
                return function.apply(input);
            }
        };
    }

    /**
     * Creates an INTEGER Printer that wraps the function provided
     * @param function  the function to wrap
     * @return          the newly created function Printer
     */
    public static Printer<Integer> forInt(IntFunction<String> function) {
        return new Printer<Integer>(FunctionStyle.INTEGER, DEFAULT_NULL) {
            @Override
            public final String apply(int input) {
                return function.apply(input);
            }
        };
    }

    /**
     * Creates an LONG Printer that wraps the function provided
     * @param function  the function to wrap
     * @return          the newly created function Printer
     */
    public static Printer<Long> forLong(LongFunction<String> function) {
        return new Printer<Long>(FunctionStyle.LONG, DEFAULT_NULL) {
            @Override
            public final String apply(long input) {
                return function.apply(input);
            }
        };
    }

    /**
     * Creates an DOUBLE Printer that wraps the function provided
     * @param function  the function to wrap
     * @return          the newly created function Printer
     */
    public static Printer<Double> forDouble(DoubleFunction<String> function) {
        return new Printer<Double>(FunctionStyle.DOUBLE, DEFAULT_NULL) {
            @Override
            public final String apply(double input) {
                return function.apply(input);
            }
        };
    }

    /**
     * Creates an OBJECT Printer that wraps the function provided
     * @param function  the function to wrap
     * @return          the newly created function Printer
     */
    public static <T> Printer<T> forObject(Function<T,String> function) {
        return new Printer<T>(FunctionStyle.OBJECT, DEFAULT_NULL) {
            @Override
            public final String apply(T input) {
                return function.apply(input);
            }
        };
    }

    /**
     * Returns a Printer for values of type Boolean
     * @return      thw newly created Printer
     */
    public static Printer<Boolean> ofBoolean() {
        return new PrinterOfPrimitive<>(FunctionStyle.BOOLEAN);
    }

    /**
     * Returns a Printer for values of type Integer
     * @return      thw newly created Printer
     */
    public static Printer<Integer> ofInt() {
        return new PrinterOfPrimitive<>(FunctionStyle.INTEGER);
    }

    /**
     * Returns a Printer for values of type Long
     * @return      thw newly created Printer
     */
    public static Printer<Long> ofLong() {
        return new PrinterOfPrimitive<>(FunctionStyle.LONG);
    }

    /**
     * Returns a Printer for values of type Double
     * @param pattern   the decimal format pattern
     * @return      thw newly created Printer
     */
    public static Printer<Double> ofDouble(String pattern) {
        return ofDouble(pattern, 1);
    }

    /**
     * Returns a Printer for values of type Double
     * @param pattern   the decimal format pattern
     * @param multipler the multiplier
     * @return      thw newly created Printer
     */
    public static Printer<Double> ofDouble(String pattern, int multipler) {
        return ofDouble(createDecimalFormat(pattern, multipler));
    }

    /**
     * Returns a Printer for values of type Double
     * @param decimalFormat the decimal format
     * @return      thw newly created Printer
     */
    public static Printer<Double> ofDouble(DecimalFormat decimalFormat) {
        return new PrinterOfPrimitive<>(FunctionStyle.DOUBLE, () -> decimalFormat);
    }

    /**
     * Returns a Printer for values of type BigDecimal
     * @return      thw newly created Printer
     */
    public static Printer<BigDecimal> ofBigDecimal() {
        return new PrinterOfBigDecimal(DEFAULT_NULL);
    }

    /**
     * Returns a Printer for values of type Enum
     * @return      thw newly created Printer
     */
    public static <T extends Enum> Printer<T> ofEnum() {
        return new PrinterOfEnum<>(DEFAULT_NULL);
    }

    /**
     * Returns a Printer for values of type String
     * @return      thw newly created Printer
     */
    public static Printer<String> ofString() {
        return new PrinterOfString(DEFAULT_NULL);
    }

    /**
     * Returns a Printer for values of type String
     * @return      thw newly created Printer
     */
    public static Printer<Date> ofDate(String pattern) {
        return ofDate(new SimpleDateFormat(pattern));
    }

    /**
     * Returns a Printer for values of type Date
     * @return      thw newly created Printer
     */
    public static Printer<Date> ofDate(DateFormat format) {
        return new PrinterOfDate(DEFAULT_NULL, () -> format);
    }

    /**
     * Returns a Printer for values of type LocalTime
     * @return      thw newly created Printer
     */
    public static Printer<LocalTime> ofLocalTime(String pattern) {
        return ofLocalTime(DateTimeFormatter.ofPattern(pattern));
    }

    /**
     * Returns a Printer for values of type LocalTime
     * @return      thw newly created Printer
     */
    public static Printer<LocalTime> ofLocalTime(DateTimeFormatter formatter) {
        return new PrinterOfTemporal<>(DEFAULT_NULL, () -> formatter);
    }

    /**
     * Returns a Printer for values of type LocalDate
     * @return      thw newly created Printer
     */
    public static Printer<LocalDate> ofLocalDate(String pattern) {
        return ofLocalDate(DateTimeFormatter.ofPattern(pattern));
    }

    /**
     * Returns a Printer for values of type LocalDate
     * @return      thw newly created Printer
     */
    public static Printer<LocalDate> ofLocalDate(DateTimeFormatter formatter) {
        return new PrinterOfTemporal<>(DEFAULT_NULL, () -> formatter);
    }

    /**
     * Returns a Printer for values of type LocalDateTime
     * @return      thw newly created Printer
     */
    public static Printer<LocalDateTime> ofLocalDateTime(String pattern) {
        return ofLocalDateTime(DateTimeFormatter.ofPattern(pattern));
    }

    /**
     * Returns a Printer for values of type LocalDateTime
     * @return      thw newly created Printer
     */
    public static Printer<LocalDateTime> ofLocalDateTime(DateTimeFormatter formatter) {
        return new PrinterOfTemporal<>(DEFAULT_NULL, () -> formatter);
    }

    /**
     * Returns a Printer for values of type ZonedDateTime
     * @return      thw newly created Printer
     */
    public static Printer<ZonedDateTime> ofZonedDateTime(String pattern, ZoneId zoneId) {
        return ofZonedDateTime(DateTimeFormatter.ofPattern(pattern).withZone(zoneId));
    }

    /**
     * Returns a Printer for values of type LocalDateTime
     * @return      thw newly created Printer
     */
    public static Printer<ZonedDateTime> ofZonedDateTime(DateTimeFormatter formatter) {
        return new PrinterOfTemporal<>(DEFAULT_NULL, () -> formatter);
    }

    /**
     * Returns a Printer for values of type TimeZone
     * @return      thw newly created Printer
     */
    public static Printer<TimeZone> ofTimeZone() {
        return new PrinterOfTimeZone(DEFAULT_NULL);
    }

    /**
     * Returns a Printer for values of type ZoneId
     * @return      thw newly created Printer
     */
    public static Printer<ZoneId> ofZoneId() {
        return new PrinterOfZoneId(DEFAULT_NULL);
    }

    /**
     * Returns a Printer for values of type Period
     * @return      thw newly created Printer
     */
    public static Printer<Period> ofPeriod() {
        return new PrinterOfPeriod(DEFAULT_NULL);
    }

    /**
     * Returns a Printer for values of type Object
     * @return      thw newly created Printer
     */
    public static Printer<Object> ofObject() {
        return new PrinterOfObject(DEFAULT_NULL);
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
