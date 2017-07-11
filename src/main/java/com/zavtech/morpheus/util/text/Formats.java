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
package com.zavtech.morpheus.util.text;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.zavtech.morpheus.util.Asserts;
import com.zavtech.morpheus.util.functions.ToBooleanFunction;
import com.zavtech.morpheus.util.text.parser.Parser;
import com.zavtech.morpheus.util.text.printer.Printer;

/**
 * A class that manages a collection of data type specific Parsers & Printers for reading & writing textual data.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class Formats {

    private final Map<Object,Parser<?>> parserMap = new LinkedHashMap<>();
    private final Map<Object,Printer<?>> printerMap = new LinkedHashMap<>();

    private Supplier<String> nullValue = () -> "null";
    private Set<String> nullSet = new HashSet<>(Arrays.asList("null", "NULL", "Null", "N/A", "n/a", "-"));
    private ToBooleanFunction<String> nullCheck = value -> value == null || value.trim().length() == 0 || nullSet.contains(value);

    /**
     * Constructor
     */
    public Formats() {
        this.registerParsers();
        this.registerPrinters();
    }

    /**
     * Registers the default parsers for these formats
     * NOTE THAT THE ORDER IN WHICH THESE ARE REGISTERED MATTERS SOMEWHAT (MORE SPECIFIC TO LESS SPECIFIC)
     */
    private void registerParsers() {
        this.setParser(boolean.class, Parser.ofBoolean().withNullChecker(nullCheck));
        this.setParser(Boolean.class, Parser.ofBoolean().withNullChecker(nullCheck));
        this.setParser(int.class, Parser.ofInteger().withNullChecker(nullCheck));
        this.setParser(Integer.class, Parser.ofInteger().withNullChecker(nullCheck));
        this.setParser(long.class, Parser.ofLong().withNullChecker(nullCheck));
        this.setParser(Long.class, Parser.ofLong().withNullChecker(nullCheck));
        this.setParser(double.class, Parser.ofDouble("0.0000####;-0.0000####", 1).withNullChecker(nullCheck));
        this.setParser(Double.class, Parser.ofDouble("0.0000####;-0.0000####", 1).withNullChecker(nullCheck));
        this.setParser(BigDecimal.class, Parser.ofBigDecimal().withNullChecker(nullCheck));
        this.setParser(LocalDate.class, Parser.ofLocalDate(DateTimeFormatter.ISO_LOCAL_DATE).withNullChecker(nullCheck));
        this.setParser(LocalTime.class, Parser.ofLocalTime(DateTimeFormatter.ISO_LOCAL_TIME).withNullChecker(nullCheck));
        this.setParser(LocalDateTime.class, Parser.ofLocalDateTime(DateTimeFormatter.ISO_LOCAL_DATE_TIME).withNullChecker(nullCheck));
        this.setParser(ZonedDateTime.class, Parser.ofZonedDateTime(DateTimeFormatter.ISO_ZONED_DATE_TIME).withNullChecker(nullCheck));
        this.setParser(Period.class, Parser.ofPeriod().withNullChecker(nullCheck));
        this.setParser(ZoneId.class, Parser.ofZoneId().withNullChecker(nullCheck));
        this.setParser(Month.class, Parser.ofEnum(Month.class).withNullChecker(nullCheck));
        this.setParser(TimeZone.class, Parser.ofTimeZone().withNullChecker(nullCheck));
        this.setParser(java.util.Date.class, Parser.ofDate().withNullChecker(nullCheck));
        this.setParser(String.class, Parser.ofString().withNullChecker(nullCheck));
        this.setParser(Object.class, Parser.ofObject().withNullChecker(nullCheck));
    }

    /**
     * Registers the default printers for these formats
     * NOTE THAT THE ORDER IN WHICH THESE ARE REGISTERED MATTERS SOMEWHAT (MORE SPECIFIC TO LESS SPECIFIC)
     */
    private void registerPrinters() {
        this.setPrinter(boolean.class, Printer.ofBoolean().withNullValue(nullValue));
        this.setPrinter(Boolean.class, Printer.ofBoolean().withNullValue(nullValue));
        this.setPrinter(int.class, Printer.ofInt().withNullValue(nullValue));
        this.setPrinter(Integer.class, Printer.ofInt().withNullValue(nullValue));
        this.setPrinter(long.class, Printer.ofLong().withNullValue(nullValue));
        this.setPrinter(Long.class, Printer.ofLong().withNullValue(nullValue));
        this.setPrinter(double.class, Printer.ofDouble("0.0000####;-0.0000####").withNullValue(nullValue));
        this.setPrinter(Double.class, Printer.ofDouble("0.0000####;-0.0000####").withNullValue(nullValue));
        this.setPrinter(LocalDate.class, Printer.ofLocalDate(DateTimeFormatter.ISO_DATE).withNullValue(nullValue));
        this.setPrinter(LocalTime.class, Printer.ofLocalTime(DateTimeFormatter.ISO_TIME).withNullValue(nullValue));
        this.setPrinter(LocalDateTime.class, Printer.ofLocalDateTime(DateTimeFormatter.ISO_DATE_TIME).withNullValue(nullValue));
        this.setPrinter(ZonedDateTime.class, Printer.ofZonedDateTime(DateTimeFormatter.ISO_ZONED_DATE_TIME).withNullValue(nullValue));
        this.setPrinter(Period.class, Printer.ofPeriod().withNullValue(nullValue));
        this.setPrinter(ZoneId.class, Printer.ofZoneId().withNullValue(nullValue));
        this.setPrinter(Month.class, Printer.ofEnum().withNullValue(nullValue));
        this.setPrinter(TimeZone.class, Printer.ofTimeZone().withNullValue(nullValue));
        this.setPrinter(java.util.Date.class, Printer.ofDate("yyyy-MM-dd").withNullValue(nullValue));
        this.setPrinter(java.sql.Date.class, Printer.ofDate("yyyy-MM-dd").withNullValue(nullValue));
        this.setPrinter(Object.class, Printer.ofObject().withNullValue(nullValue));
        this.setPrinter(String.class, Printer.ofString().withNullValue(nullValue));
    }

    /**
     * Creates a new Formats instance, with additional options
     * @param options   the consumer to set additional options
     * @return          the newly created formats
     */
    public static Formats of(Consumer<Formats> options) {
        final Formats formats = new Formats();
        options.accept(formats);
        return formats;
    }

    /**
     * Returns the set of parser keys
     * @return  the set of parser keys
     */
    public Set<Object> getParserKeys() {
        return Collections.unmodifiableSet(parserMap.keySet());
    }

    /**
     * Returns the set of printer keys
     * @return  the set of printer keys
     */
    public Set<Object> getPrinterKeys() {
        return Collections.unmodifiableSet(printerMap.keySet());
    }

    /**
     * Sets the string values that should be considered nulls
     * @param nullValues    the null value strings
     */
    public void setNullValues(String... nullValues) {
        this.nullSet.clear();
        this.nullSet.addAll(Arrays.asList(nullValues));
    }

    /**
     * Sets a Parser for the key and type specified
     * @param key       the key for parser
     * @param parser    the parser function
     * @param <T>       the parser type
     */
    public <T> void setParser(Object key, Parser<T> parser) {
        Asserts.notNull(key, "The parser key cannot be null");
        Asserts.notNull(parser, "The parser function cannot be null");
        this.parserMap.put(key, parser);
    }

    /**
     * Sets a Parser for the key and type specified
     * @param key       the key for parser
     * @param type      the type for an existing parser
     * @param <T>       the parser type
     */
    public <T> void setParser(Object key, Class<?> type) {
        Asserts.notNull(key, "The parser key cannot be null");
        Asserts.notNull(type, "The parser type cannot be null");
        final Optional<Parser<?>> parser = parserMap.values().stream().filter(p -> p.getType().equals(type)).findFirst();
        if (parser.isPresent()) {
            this.parserMap.put(key, parser.get());
        } else {
            throw new IllegalArgumentException("No Parser exists for type:" + type.getSimpleName());
        }
    }

    /**
     * Sets a Printer for the key and type specified
     * @param key       the key for printer
     * @param printer   the printer reference
     * @param <T>       the printer data type
     */
    public <T> void setPrinter(Object key, Printer<T> printer) {
        Asserts.notNull(key, "The printer key cannot be null");
        Asserts.notNull(printer, "The printer function cannot be null");
        this.printerMap.put(key, printer);
    }

    /**
     * Sets a Parser/Printer pair for a LocalTime using the pattern specified
     * This register a Printer/Parser pair for the LocalTime class key
     * @param pattern   the local date format pattern
     */
    public void setTimeFormat(String pattern) {
        Asserts.notNull(pattern, "The format pattern cannot be null");
        this.setPrinter(LocalTime.class, Printer.ofLocalTime(pattern).withNullValue(nullValue));
        this.setParser(LocalTime.class, Parser.ofLocalTime(pattern).withNullChecker(nullCheck));
    }

    /**
     * Sets a Parser/Printer pair for a LocalDate using the pattern specified
     * This register a Printer/Parser pair for the LocalDate class key
     * @param pattern   the local date format pattern
     */
    public void setDateFormat(String pattern) {
        Asserts.notNull(pattern, "The format pattern cannot be null");
        this.setPrinter(LocalDate.class, Printer.ofLocalDate(pattern).withNullValue(nullValue));
        this.setParser(LocalDate.class, Parser.ofLocalDateTime(pattern).withNullChecker(nullCheck));
    }

    /**
     * Sets a Parser/Printer pair for a LocalDateTime using the pattern specified
     * This register a Printer/Parser pair for the LocalDateTime class key
     * @param pattern   the local date format pattern
     */
    public void setDateTimeFormat(String pattern) {
        Asserts.notNull(pattern, "The format pattern cannot be null");
        this.setPrinter(LocalDateTime.class, Printer.ofLocalDateTime(pattern).withNullValue(nullValue));
        this.setParser(LocalDateTime.class, Parser.ofLocalDateTime(pattern).withNullChecker(nullCheck));
    }

    /**
     * Sets a Parser/Printer pair for a ZonedDateTime using the pattern specified
     * This register a Printer/Parser pair for the ZonedDateTime class key
     * @param pattern   the local date format pattern
     * @param zoneId    the zone identifier
     */
    public void setDateTimeFormat(String pattern, ZoneId zoneId) {
        Asserts.notNull(pattern, "The format pattern cannot be null");
        this.setPrinter(ZonedDateTime.class, Printer.ofZonedDateTime(pattern, zoneId).withNullValue(nullValue));
        this.setParser(ZonedDateTime.class, Parser.ofZonedDateTime(pattern, zoneId).withNullChecker(nullCheck));
    }

    /**
     * Sets a Parser/Printer pair for a LocalTime using the pattern specified
     * @param key       the key for the Printer & Parser
     * @param pattern   the local date format pattern
     */
    public void setTimeFormat(Object key, String pattern) {
        Asserts.notNull(key, "The Parser/Printer key cannot be null");
        Asserts.notNull(pattern, "The format pattern cannot be null");
        this.setPrinter(key, Printer.ofLocalTime(pattern).withNullValue(nullValue));
        this.setParser(key, Parser.ofLocalTime(pattern).withNullChecker(nullCheck));
    }

    /**
     * Sets a Parser/Printer pair for a LocalDate using the pattern specified
     * @param key       the key for the Printer & Parser
     * @param pattern   the local date format pattern
     */
    public void setDateFormat(Object key, String pattern) {
        Asserts.notNull(key, "The Parser/Printer key cannot be null");
        Asserts.notNull(pattern, "The format pattern cannot be null");
        this.setPrinter(key, Printer.ofLocalDate(pattern).withNullValue(nullValue));
        this.setParser(key, Parser.ofLocalDateTime(pattern).withNullChecker(nullCheck));
    }

    /**
     * Sets a Parser/Printer pair for a LocalDateTime using the pattern specified
     * @param key       the key for the Printer & Parser
     * @param pattern   the local date format pattern
     */
    public void setDateTimeFormat(Object key, String pattern) {
        Asserts.notNull(key, "The Parser/Printer key cannot be null");
        Asserts.notNull(pattern, "The format pattern cannot be null");
        this.setPrinter(key, Printer.ofLocalDate(pattern).withNullValue(nullValue));
        this.setParser(key, Parser.ofLocalDateTime(pattern).withNullChecker(nullCheck));
    }

    /**
     * Sets a Parser/Printer pair for a ZonedDateTime using the pattern specified
     * @param key       the key for the Printer & Parser
     * @param pattern   the local date format pattern
     * @param zoneId    the zone identifier
     */
    public void setDateTimeFormat(Object key, String pattern, ZoneId zoneId) {
        Asserts.notNull(key, "The Parser/Printer key cannot be null");
        Asserts.notNull(pattern, "The format pattern cannot be null");
        this.setParser(key, Parser.ofZonedDateTime(pattern, zoneId).withNullChecker(nullCheck));
        this.setPrinter(key, Printer.ofZonedDateTime(pattern, zoneId).withNullValue(nullValue));
    }

    /**
     * Sets a Parser/Printer pair function against the key specified
     * @param pattern       the decimal pattern, as per java.text.DecimalFormat
     * @param multiplier    the multiplier as per java.text.DecimalFormat
     */
    public void setDecimalFormat(String pattern, int multiplier) {
        Asserts.notNull(pattern, "The decimal pattern cannot be null");
        this.setParser(double.class, Parser.ofDouble(pattern, multiplier).withNullChecker(nullCheck));
        this.setParser(Double.class, Parser.ofDouble(pattern, multiplier).withNullChecker(nullCheck));
        this.setPrinter(double.class, Printer.ofDouble(pattern, multiplier).withNullValue(nullValue));
        this.setPrinter(Double.class, Printer.ofDouble(pattern, multiplier).withNullValue(nullValue));
    }

    /**
     * Sets a Parser/Printer pair function against the key specified
     * @param key           the key for printer & parser
     * @param pattern       the decimal pattern, as per java.text.DecimalFormat
     * @param multiplier    the multiplier as per java.text.DecimalFormat
     */
    public void setDecimalFormat(Object key, String pattern, int multiplier) {
        Asserts.notNull(key, "The Parser/Printer key cannot be null");
        Asserts.notNull(pattern, "The decimal pattern cannot be null");
        this.setParser(key, Parser.ofDouble(pattern, multiplier).withNullChecker(nullCheck));
        this.setPrinter(key, Printer.ofDouble(pattern, multiplier).withNullValue(nullValue));
    }

    /**
     * Copies an existing Parser to another key, while retaining the existing mapping
     * @param existingKey   the existing parser key
     * @param newKey        the new parser key
     * @throws IllegalArgumentException if no parser exists for existing key
     */
    public <T> Formats copyParser(Object existingKey, Object newKey) {
        Asserts.notNull(newKey, "The new key cannot be null");
        Asserts.notNull(existingKey, "The existing key cannot be null");
        final Parser<?> parser = getParser(existingKey);
        if (parser != null) {
            this.parserMap.put(newKey, parser);
            return this;
        } else {
            throw new IllegalArgumentException("No parser found for key: " + existingKey);
        }
    }

    /**
     * Copies an existing Printer to another key, while retaining the existing mapping
     * @param existingKey   the existing parser key
     * @param newKey        the new parser key
     * @throws IllegalArgumentException if no printer exists for existing key
     */
    public <T> void copyPrinter(Class<T> existingKey, Object newKey) {
        Asserts.notNull(newKey, "The new key cannot be null");
        Asserts.notNull(existingKey, "The existing key cannot be null");
        final Printer<?> printer = getPrinter(existingKey);
        if (printer != null) {
            this.printerMap.put(newKey, printer);
        } else {
            throw new IllegalArgumentException("No printer found for key: " + existingKey);
        }
    }

    /**
     * Returns a formatted string of the value using the appropriate encoder for type
     * @param value     the value to format
     * @param <T>       the type
     * @return          the formatted string
     */
    @SuppressWarnings("unchecked")
    public <T> String format(T value) {
        if (value == null) {
            return "null";
        } else {
            final Class<T> type = (Class<T>)value.getClass();
            return getPrinterOrFail(type, null).apply(value);
        }
    }

    /**
     * Returns a Printer for the key specified
     * @param key   the key to lookup Printer against
     * @param <T>   the Printer type
     * @return      the printer match, null if no match
     */
    @SuppressWarnings("unchecked")
    public <T> Printer<T> getPrinter(Object key) {
        final Printer<T> printer = (Printer<T>)printerMap.get(key);
        if (printer != null) {
            return printer;
        } else if (key instanceof Class) {
            final Class<T> type = (Class<T>)key;
            if (type.isEnum()) {
                final Printer<?> enumPrinter = Printer.ofEnum().withNullValue(nullValue);
                this.printerMap.put(key, enumPrinter);
                return (Printer<T>)enumPrinter;
            }
        }
        return null;
    }

    /**
     * Returns a Printer for the key specified
     * @param key   the key to lookup Printer against
     * @param <T>   the Printer type
     * @return      the Printer match
     * @throws IllegalArgumentException   if no printer exists for key
     */
    @SuppressWarnings("unchecked")
    public <T> Printer<T> getPrinterOrFail(Object key) {
        final Printer<T> printer = getPrinter(key);
        if (printer != null) {
            return printer;
        } else {
            throw new IllegalArgumentException("No Printer has been registered for key: " + key);
        }
    }

    /**
     * Returns a Printer for the key specified
     * @param key   the key to lookup Printer against
     * @param <T>   the Printer type
     * @return      the printer match
     * @throws IllegalArgumentException   if no printer exists for key
     */
    @SuppressWarnings("unchecked")
    public <T> Printer<T> getPrinterOrFail(Object key, Object defaultKey) {
        final Printer<T> printer = getPrinter(key);
        if (printer != null) {
            return printer;
        } else {
            final Printer<T> fallback = getPrinter(defaultKey);
            if (fallback != null) {
                return fallback;
            } else {
                throw new IllegalArgumentException("No format has been registered for type: " + key);
            }
        }
    }

    /**
     * Returns a Parser for the key specified
     * @param key   the parser key
     * @param <T>   the type for parser
     * @return      the Parser match, null if no match
     */
    @SuppressWarnings("unchecked")
    public <T> Parser<T> getParser(Object key) {
        final Parser<T> parser = (Parser<T>)parserMap.get(key);
        if (parser != null) {
            return parser;
        } else if (key instanceof Class && ((Class)key).isEnum()) {
            final Class<Enum> enumClass = (Class<Enum>)key;
            final Parser<T> enumParser = (Parser<T>)Parser.ofEnum(enumClass).withNullChecker(nullCheck);
            this.parserMap.put(key, enumParser);
            return enumParser;
        } else {
            return null;
        }
    }

    /**
     * Returns a Parser for the key specified
     * @param key   the parser key
     * @param <T>   the type for parser
     * @return      the Parser match
     * @throws IllegalArgumentException   if no parser exists for key
     */
    @SuppressWarnings("unchecked")
    public <T> Parser<T> getParserOrFail(Object key) {
        final Parser<T> parser = getParser(key);
        if (parser != null) {
            return parser;
        } else {
            throw new IllegalArgumentException("No Parser has been registered for key: " + key);
        }
    }

    /**
     * Returns a parser for the key specified
     * @param key           the parser key
     * @param defaultKey    the default key if no match for primary key
     * @param <T>           the type for parser
     * @return              the parser match
     */
    @SuppressWarnings("unchecked")
    public <T> Parser<T> getParserOrFail(Object key, Object defaultKey) {
        final Parser<T> parser = (Parser<T>)parserMap.get(key);
        if (parser != null) {
            return parser;
        } else if (defaultKey == null) {
            return null;
        } else {
            final Parser<T> defaultParser = (Parser<T>)parserMap.get(defaultKey);
            if (defaultParser == null) {
                throw new RuntimeException("No parser configured in Formats for " + key + " or " + defaultKey);
            } else {
                return defaultParser;
            }
        }
    }

    /**
     * Finds a Parser that can parse all the values in the collection
     * @param values    the values to attempt to parse
     * @return          the Parser that can parse all values
     */
    @SuppressWarnings("unchecked")
    public Optional<Parser<?>> findParser(String... values) {
        return findParser(Arrays.asList(values));
    }

    /**
     * Finds a Parser that can parse all the values in the collection
     * @param values    the values to attempt to parse
     * @return          the Parser that can parse all values
     */
    @SuppressWarnings("unchecked")
    public Optional<Parser<?>> findParser(Collection<String> values) {
        Objects.requireNonNull(values, "The values to parse cannot be null");
        final List<String> nonNullValues = values.stream().filter(v -> !nullCheck.applyAsBoolean(v)).collect(Collectors.toList());
        if (nonNullValues.size() == 0) {
            return Optional.empty();
        } else {
            for (Map.Entry<Object,Parser<?>> entry : parserMap.entrySet()) {
                final Parser<?> parser = entry.getValue();
                final boolean allMatch = nonNullValues.stream().allMatch(parser::isSupported);
                if (allMatch && !parser.getType().equals(Object.class)) {
                    return Optional.of(parser.optimize(nonNullValues.iterator().next()));
                }
            }
            return Optional.empty();
        }
    }
}
