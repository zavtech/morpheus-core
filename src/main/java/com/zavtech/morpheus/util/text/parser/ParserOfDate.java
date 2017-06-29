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

import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.zavtech.morpheus.util.functions.FunctionStyle;
import com.zavtech.morpheus.util.functions.ToBooleanFunction;
import com.zavtech.morpheus.util.text.FormatException;

/**
 * A Parser implementation for Date objects
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class ParserOfDate<T extends java.util.Date> extends Parser<T> {

    private static final Map<Pattern,DateTimeFormatter> patternMap = new LinkedHashMap<>();

    private Class<?> dateClass;
    private Calendar calendar = Calendar.getInstance();
    private Matcher matcher1 = Pattern.compile("(\\d{4})-(\\d{2})-(\\d{2})").matcher("");

    /**
     * Static initializer
     */
    static {
        patternMap.put(Pattern.compile("\\d{4}-\\d{2}-\\d{2}"), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        patternMap.put(Pattern.compile("\\d{2}-\\p{Alpha}{3}]-\\d{4}"), DateTimeFormatter.ofPattern("dd-MMM-yyyy"));
    }


    /**
     * Constructor
     * @param dateClass     the date class
     * @param nullChecker   the null checker function
     */
    ParserOfDate(Class<T> dateClass, ToBooleanFunction<String> nullChecker) {
        super(FunctionStyle.OBJECT, dateClass, nullChecker);
        this.dateClass = dateClass;
    }

    @Override
    public final boolean isSupported(String value) {
        if (!getNullChecker().applyAsBoolean(value)) {
            for (Map.Entry<Pattern,DateTimeFormatter> entry : patternMap.entrySet()) {
                final Matcher matcher = entry.getKey().matcher(value);
                if (matcher.reset(value).matches()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override()
    @SuppressWarnings("unchecked")
    public final T apply(String value) {
        try {
            if (getNullChecker().applyAsBoolean(value)) {
                return null;
            } else {
                final long epochMillis = toEpochMillis(value);
                if (dateClass.equals(java.sql.Date.class)) {
                    return (T)new java.sql.Date(epochMillis);
                } else if (dateClass.equals(java.util.Date.class)) {
                    return (T)new java.util.Date(epochMillis);
                } else {
                    throw new IllegalStateException("Unsupported date class: " + dateClass);
                }
            }
        } catch (Exception ex) {
            throw new FormatException("Failed to parse value into Date: " + value, ex);
        }
    }

    /**
     * Parses a string and returns UTC Epoch Millis
     * @param value     the value to parse
     * @return          the UTC epoch millis
     */
    private long toEpochMillis(String value) {
        if (matcher1.reset(value).matches()) {
            calendar.set(Calendar.YEAR, Integer.parseInt(matcher1.group(1)));
            calendar.set(Calendar.MONTH, Integer.parseInt(matcher1.group(2))-1);
            calendar.set(Calendar.DATE, Integer.parseInt(matcher1.group(3)));
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            return calendar.getTimeInMillis();
        } else {
            throw new IllegalArgumentException("Unrecognized date format for value: " + value);
        }
    }
}
