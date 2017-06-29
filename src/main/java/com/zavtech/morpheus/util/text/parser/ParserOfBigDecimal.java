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

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.zavtech.morpheus.util.functions.FunctionStyle;
import com.zavtech.morpheus.util.functions.ToBooleanFunction;
import com.zavtech.morpheus.util.text.FormatException;

/**
 * A Parser implementation for BigDecimal objects
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class ParserOfBigDecimal extends Parser<BigDecimal> {

    private static final Set<Pattern> patternSet = new HashSet<>();

    /**
     * Static initializer
     */
    static {
        patternSet.add(Pattern.compile("NaN|[-+]?[0-9]+\\.?[0-9]*"));
        patternSet.add(Pattern.compile("[-+]?[0-9]+\\.?[0-9]*([Ee][+-]?[0-9]+)?"));
    }

    /**
     * Constructor
     * @param nullChecker   the null checker function
     */
    ParserOfBigDecimal(ToBooleanFunction<String> nullChecker) {
        super(FunctionStyle.OBJECT, BigDecimal.class, nullChecker);
    }

    @Override
    public final boolean isSupported(String value) {
        if (!getNullChecker().applyAsBoolean(value)) {
            for (Pattern pattern : patternSet) {
                final Matcher matcher = pattern.matcher(value);
                if (matcher.reset(value).matches()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public BigDecimal apply(String value) {
        try {
            return getNullChecker().applyAsBoolean(value) ? null : new BigDecimal(value);
        } catch (Exception ex) {
            throw new FormatException("Failed to parse value into BigDecimal: " + value, ex);
        }
    }
}
