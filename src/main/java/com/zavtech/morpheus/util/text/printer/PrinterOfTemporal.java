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

import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.function.Supplier;

import com.zavtech.morpheus.util.functions.FunctionStyle;

/**
 * A generalized Printer that supports any instance that implements TemporalAccessor.
 *
 * @param <T>   the TemporalAccessor type for this printer
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class PrinterOfTemporal<T extends TemporalAccessor> extends Printer<T> {

    private Supplier<DateTimeFormatter> format;

    /**
     * Constructor
     * @param nullValue     the value to print for nulls
     * @param format        the date format to print values
     */
    PrinterOfTemporal(Supplier<String> nullValue, Supplier<DateTimeFormatter> format) {
        super(FunctionStyle.OBJECT, nullValue);
        this.format = format;
    }

    @Override
    public final String apply(T value) {
        if (value == null) {
            return getNullValue().get();
        } else {
            final DateTimeFormatter formatter = format.get();
            return formatter != null ? formatter.format(value) : value.toString();
        }
    }
}
