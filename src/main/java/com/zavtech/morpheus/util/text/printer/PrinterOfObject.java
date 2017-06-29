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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Supplier;

import com.zavtech.morpheus.util.Asserts;
import com.zavtech.morpheus.util.functions.FunctionStyle;

/**
 * A Printer implementation that can print any object with appropriate formatting
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class PrinterOfObject extends Printer<Object> {

    private Map<Class<?>,Printer<?>> printerMap = new LinkedHashMap<>();

    /**
     * Constructor
     * @param nullValue the null value supplier
     */
    PrinterOfObject(Supplier<String> nullValue) {
        super(FunctionStyle.OBJECT, nullValue);
        this.printerMap.put(boolean.class, Printer.ofBoolean());
        this.printerMap.put(Boolean.class, Printer.ofBoolean());
        this.printerMap.put(int.class, Printer.ofInt());
        this.printerMap.put(Integer.class, Printer.ofInt());
        this.printerMap.put(long.class, Printer.ofLong());
        this.printerMap.put(Long.class, Printer.ofLong());
        this.printerMap.put(double.class, Printer.ofDouble("0.000###;-0.000###"));
        this.printerMap.put(Double.class, Printer.ofDouble("0.000###;-0.000###"));
        this.printerMap.put(LocalDate.class, Printer.ofLocalDate(DateTimeFormatter.ISO_LOCAL_DATE));
        this.printerMap.put(LocalTime.class, Printer.ofLocalTime(DateTimeFormatter.ISO_LOCAL_TIME));
        this.printerMap.put(LocalDateTime.class, Printer.ofLocalDateTime(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        this.printerMap.put(ZonedDateTime.class, Printer.ofZonedDateTime(DateTimeFormatter.ISO_ZONED_DATE_TIME));
        this.printerMap.put(Period.class, Printer.ofPeriod());
        this.printerMap.put(ZoneId.class, Printer.ofZoneId());
        this.printerMap.put(TimeZone.class, Printer.ofTimeZone());
        this.printerMap.put(java.util.Date.class, Printer.ofDate("yyyy-MM-dd"));
        this.printerMap.put(java.sql.Date.class, Printer.ofDate("yyyy-MM-dd"));
    }

    /**
     * Adds a type specific printer to this generic printer
     * @param type      the type class
     * @param printer   the printer for type
     * @param <T>       the type
     */
    public <T> void add(Class<T> type, Printer<T> printer) {
        Asserts.notNull(type, "The type class cannot be null");
        Asserts.notNull(printer, "The printer instance cannot be null");
        this.printerMap.put(type, printer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public final String apply(Object input) {
        if (input == null) {
            final String nullString = getNullValue().get();
            return nullString != null ? nullString : "null";
        } else {
            final Class<?> type = input.getClass();
            final Printer<Object> printer = (Printer<Object>)printerMap.get(type);
            return printer != null ? printer.apply(input) : input.toString();
        }
    }
}
