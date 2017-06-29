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

import java.text.DecimalFormat;
import java.util.function.Supplier;

import com.zavtech.morpheus.util.functions.FunctionStyle;

/**
 * A Printer implementation that supports printing of primitive boolean, int, long and double
 *
 * @param <T>   the type for this printer
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class PrinterOfPrimitive<T> extends Printer<T> {

    private Supplier<DecimalFormat> decimalFormat;

    /**
     * Constructor
     * @param style         the function style
     */
    PrinterOfPrimitive(FunctionStyle style) {
        this(style, null);
    }

    /**
     * Constructor
     * @param style         the function style
     * @param decimalFormat the optional decimal format
     */
    PrinterOfPrimitive(FunctionStyle style, Supplier<DecimalFormat> decimalFormat) {
        super(style, () -> "null");
        this.decimalFormat = decimalFormat;
    }

    @Override
    public final String apply(boolean input) {
        return String.valueOf(input);
    }

    @Override
    public final String apply(int input) {
        return String.valueOf(input);
    }

    @Override
    public final String apply(long input) {
        return String.valueOf(input);
    }

    @Override
    public final String apply(double input) {
        if (Double.isNaN(input)) {
            return "NaN";
        } else {
            final DecimalFormat format = decimalFormat.get();
            return format != null ? format.format(input) : String.valueOf(input);
        }
    }
}
