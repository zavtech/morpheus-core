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

import java.time.Period;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.zavtech.morpheus.util.functions.FunctionStyle;
import com.zavtech.morpheus.util.functions.ToBooleanFunction;
import com.zavtech.morpheus.util.text.FormatException;

/**
 * A Parser implementation for Period objects
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class ParserOfPeriod extends Parser<Period> {

    private static final Pattern pattern = Pattern.compile("(\\d)+\\s*([YMWD])", Pattern.CASE_INSENSITIVE);

    /**
     * Constructor
     * @param nullChecker   the null checker function
     */
    ParserOfPeriod(ToBooleanFunction<String> nullChecker) {
        super(FunctionStyle.OBJECT, Period.class, nullChecker);
    }

    @Override
    public final boolean isSupported(String value) {
        return !getNullChecker().applyAsBoolean(value) && pattern.matcher(value).matches();
    }

    @Override
    public final Period apply(String value) {
        try {
            if (getNullChecker().applyAsBoolean(value)) {
                return null;
            } else {
                final Matcher matcher = pattern.matcher(value);
                if (matcher.matches()) {
                    final int digits = Integer.parseInt(matcher.group(1));
                    final char code = matcher.group(2).toUpperCase().charAt(0);
                    switch (code) {
                        case 'D':   return Period.ofDays(digits);
                        case 'W':   return Period.ofWeeks(digits);
                        case 'M':   return Period.ofMonths(digits);
                        case 'Y':   return Period.ofYears(digits);
                        default:    throw new IllegalArgumentException("Unsupported period type: " + code);
                    }
                } else {
                    throw new IllegalArgumentException("Cannot parse value into an Period: " + value + " pattern: " + pattern.pattern());
                }
            }
        } catch (Exception ex) {
            throw new FormatException("Failed to parse value into Period: " + value, ex);
        }
    }
}
