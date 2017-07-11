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
 * A Parser implementation for ints
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class ParserOfInteger extends Parser<Integer> {

    private static final Pattern pattern = Pattern.compile("[-+]?[0-9]{1,10}");

    /**
     * Constructor
     * @param nullChecker   the null checker function
     */
    ParserOfInteger(ToBooleanFunction<String> nullChecker) {
        super(FunctionStyle.INTEGER, Integer.class, nullChecker);
    }

    @Override
    public Parser<Integer> optimize(String value) {
        return this;
    }

    @Override
    public final boolean isSupported(String value) {
        return !getNullChecker().applyAsBoolean(value) && pattern.matcher(value).matches();
    }

    @Override
    public final int applyAsInt(String value) {
        try {
            if (getNullChecker().applyAsBoolean(value)) {
                return 0;
            } else if (pattern.matcher(value).matches()) {
                return Integer.parseInt(value);
            } else {
                throw new IllegalArgumentException("Cannot parse value into an int: " + value + " pattern: " + pattern.pattern());
            }
        } catch (Exception ex) {
            throw new FormatException("Failed to parse value into Integer: " + value, ex);
        }
    }
}
