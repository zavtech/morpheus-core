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

import com.zavtech.morpheus.util.functions.FunctionStyle;
import com.zavtech.morpheus.util.functions.ToBooleanFunction;
import com.zavtech.morpheus.util.text.FormatException;

/**
 * A Parser implementation for Java Enums
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class ParserOfEnum<T extends Enum> extends Parser<T> {

    private Class<? extends Enum> enumClass;
    private Map<String,T> enumMap = new HashMap<>();

    /**
     * Constructor
     * @param nullChecker   the null checker function
     */
    @SuppressWarnings("unchecked")
    public ParserOfEnum(Class<T> enumClass, ToBooleanFunction<String> nullChecker) {
        super(FunctionStyle.OBJECT, enumClass, nullChecker);
        this.enumClass = enumClass;
        for (Enum value : enumClass.getEnumConstants()) {
            final String name = value.name();
            enumMap.put(name.toUpperCase(), (T)value);
        }
    }

    @Override
    public final boolean isSupported(String value) {
        return !getNullChecker().applyAsBoolean(value) && enumMap.containsKey(value.toUpperCase());
    }

    @Override
    public final T apply(String value) {
        try {
            if (getNullChecker().applyAsBoolean(value)) {
                return null;
            } else {
                final T enumValue = enumMap.get(value.toUpperCase());
                if (enumValue != null) {
                    return enumValue;
                } else {
                    throw new FormatException("Cannot parse value into " + enumClass.getSimpleName() + ":" + value);
                }
            }
        } catch (FormatException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new FormatException("Failed to parse value into " + enumClass.getSimpleName() + value, ex);
        }
    }
}
