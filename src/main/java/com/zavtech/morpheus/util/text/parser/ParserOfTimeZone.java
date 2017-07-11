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

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import com.zavtech.morpheus.util.functions.FunctionStyle;
import com.zavtech.morpheus.util.functions.ToBooleanFunction;
import com.zavtech.morpheus.util.text.FormatException;

/**
 * A Parser implementation for TimeZone objects
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class ParserOfTimeZone extends Parser<TimeZone> {

    private static final Map<String,TimeZone> timeZoneMap = new HashMap<>();

    /**
     * Static initializer
     */
    static {
        try {
            final String[] zoneNames = TimeZone.getAvailableIDs();
            for (String zoneName : zoneNames) {
                final TimeZone timeZone = TimeZone.getTimeZone(zoneName);
                timeZoneMap.put(zoneName, timeZone);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Constructor
     * @param nullChecker   the null checker function
     */
    ParserOfTimeZone(ToBooleanFunction<String> nullChecker) {
        super(FunctionStyle.OBJECT, TimeZone.class, nullChecker);
    }

    @Override
    public final boolean isSupported(String value) {
        return timeZoneMap.containsKey(value);
    }

    @Override
    public Parser<TimeZone> optimize(String value) {
        return this;
    }

    @Override
    public final TimeZone apply(String value) {
        try {
            if (getNullChecker().applyAsBoolean(value)) {
                return null;
            } else {
                final TimeZone timeZone = timeZoneMap.get(value);
                if (timeZone != null) {
                    return timeZone;
                } else {
                    throw new FormatException("Cannot parse value into an TimeZone: " + value);
                }
            }
        } catch (FormatException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new FormatException("Failed to parse value into TimeZone: " + value, ex);
        }
    }
}
