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

import java.time.ZoneId;
import java.util.function.Supplier;

import com.zavtech.morpheus.util.functions.FunctionStyle;

/**
 * A Printer implementation for ZoneId objects
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class PrinterOfZoneId extends Printer<ZoneId> {

    /**
     * Constructor
     * @param nullValue     the null value supplier
     */
    PrinterOfZoneId(Supplier<String> nullValue) {
        super(FunctionStyle.OBJECT, nullValue);
    }

    @Override
    public final String apply(ZoneId input) {
        if (input == null) {
            final String nullString = getNullValue().get();
            return nullString != null ? nullString : "null";
        } else {
            return input.getId();
        }
    }
}
