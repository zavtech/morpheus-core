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

import java.util.ArrayList;
import java.util.List;

import com.zavtech.morpheus.util.functions.FunctionStyle;
import com.zavtech.morpheus.util.functions.ToBooleanFunction;
import com.zavtech.morpheus.util.text.FormatException;

/**
 * A Parser that uses other parsers to parse content, the final parser being the string parser.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class ParserOfObject extends Parser<Object> {

    private List<Parser<?>> parserList = new ArrayList<>();

    /**
     * Constructor
     * @param nullChecker   the null checker
     */
    ParserOfObject(ToBooleanFunction<String> nullChecker) {
        super(FunctionStyle.OBJECT, Object.class, nullChecker);
        this.parserList.add(new ParserOfBoolean(nullChecker));
        this.parserList.add(new ParserOfInteger(nullChecker));
        this.parserList.add(new ParserOfLong(nullChecker));
        this.parserList.add(new ParserOfDouble(nullChecker, () -> null));
        this.parserList.add(new ParserOfLocalDate(nullChecker, () -> null));
        this.parserList.add(new ParserOfLocalTime(nullChecker, () -> null));
        this.parserList.add(new ParserOfLocalDateTime(nullChecker, () -> null));
        this.parserList.add(new ParserOfZonedDateTime(nullChecker, () -> null));
        this.parserList.add(new ParserOfPeriod(nullChecker));
        this.parserList.add(new ParserOfZoneId(nullChecker));
        this.parserList.add(new ParserOfTimeZone(nullChecker));
        this.parserList.add(new ParserOfDate<>(java.util.Date.class, nullChecker));
        this.parserList.add(new ParserOfDate<>(java.sql.Date.class, nullChecker));
        this.parserList.add(new ParserOfString(nullChecker));
    }

    @Override
    public final boolean isSupported(String value) {
        return true;
    }

    @Override
    public Parser<Object> optimize(String value) {
        return this;
    }

    @Override
    public final Object apply(String value) {
        try {
            if (getNullChecker().applyAsBoolean(value)) {
                return null;
            } else {
                for (Parser<?> parser : parserList) {
                    if (parser.isSupported(value)) {
                        return parser.apply(value);
                    }
                }
                return value;
            }
        } catch (Exception ex) {
            throw new FormatException("Failed to parse value into Object: " + value, ex);
        }
    }
}
