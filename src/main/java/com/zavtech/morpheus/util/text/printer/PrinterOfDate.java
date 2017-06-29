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

import java.text.DateFormat;
import java.util.Date;
import java.util.function.Supplier;

import com.zavtech.morpheus.util.functions.FunctionStyle;

/**
 * A Printer implementation for Date objects
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class PrinterOfDate extends Printer<Date> {

    private Supplier<DateFormat> format;

    /**
     * Constructor
     * @param nullValue     the value to print for nulls
     * @param format        the date format to print values
     */
    PrinterOfDate(Supplier<String> nullValue, Supplier<DateFormat> format) {
        super(FunctionStyle.OBJECT, nullValue);
        this.format = format;
    }

    @Override
    public final String apply(Date date) {
        if (date == null) {
            return getNullValue().get();
        } else {
            final DateFormat dateFormat = format.get();
            return dateFormat != null ? dateFormat.format(date) : date.toString();
        }
    }
}
