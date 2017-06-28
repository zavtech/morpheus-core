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

import java.util.regex.Pattern;

import com.zavtech.morpheus.util.functions.FunctionStyle;
import com.zavtech.morpheus.util.functions.ToBooleanFunction;
import com.zavtech.morpheus.util.text.FormatException;

/**
 * A Parser implementation for longs
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class ParserOfLong extends Parser<Long> {

    private static final Pattern pattern = Pattern.compile("[-+]?[0-9]{1,20}");

    /**
     * Constructor
     * @param nullChecker   the null checker function
     */
    ParserOfLong(ToBooleanFunction<String> nullChecker) {
        super(FunctionStyle.LONG, Long.class, nullChecker);
    }

    @Override
    public final boolean isSupported(String value) {
        return !getNullChecker().applyAsBoolean(value) && pattern.matcher(value).matches();
    }

    @Override
    public final long applyAsLong(String value) {
        try {
            if (getNullChecker().applyAsBoolean(value)) {
                return 0L;
            } else if (pattern.matcher(value).matches()) {
                return Long.parseLong(value);
            } else {
                throw new IllegalArgumentException("Cannot parse value into an long: " + value + " pattern: " + pattern.pattern());
            }
        } catch (Exception ex) {
            throw new FormatException("Failed to parse value into Long: " + value, ex);
        }
    }
}
