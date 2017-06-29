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

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.zavtech.morpheus.util.functions.FunctionStyle;
import com.zavtech.morpheus.util.functions.ToBooleanFunction;
import com.zavtech.morpheus.util.text.FormatException;

/**
 * A Parser implementation for ZoneId objects
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class ParserOfZoneId extends Parser<ZoneId> {

    private static final Map<String,ZoneId> zoneMap = new HashMap<>();

    /**
     * Static initializer
     */
    static {
        try {
            final Set<String> zoneNames = ZoneId.getAvailableZoneIds();
            for (String zoneName : zoneNames) {
                final ZoneId zoneId = ZoneId.of(zoneName);
                zoneMap.put(zoneName, zoneId);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Constructor
     * @param nullChecker   the null checker function
     */
    ParserOfZoneId(ToBooleanFunction<String> nullChecker) {
        super(FunctionStyle.OBJECT, ZoneId.class, nullChecker);
    }

    @Override
    public final boolean isSupported(String value) {
        return zoneMap.containsKey(value);
    }

    @Override
    public final ZoneId apply(String value) {
        try {
            if (getNullChecker().applyAsBoolean(value)) {
                return null;
            } else {
                final ZoneId zoneId = zoneMap.get(value);
                if (zoneId != null) {
                    return zoneId;
                } else {
                    throw new IllegalArgumentException("Cannot parse value into an ZoneId: " + value);
                }
            }
        } catch (Exception ex) {
            throw new FormatException("Failed to parse value into ZoneId: " + value, ex);
        }
    }
}
