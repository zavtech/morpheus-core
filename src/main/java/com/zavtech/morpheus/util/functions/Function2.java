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
package com.zavtech.morpheus.util.functions;

import java.util.function.DoubleFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.LongFunction;

/**
 * A Function Adapter that unifies all primitive functions to enable generalized APIs that can avoid primitive boxing.
 *
 * @param <I>       the input type for function
 * @param <O>       the output type for function
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public class Function2<I,O> implements Function<I,O> {

    private FunctionStyle style;

    /**
     * Construtor
     * @param style     the style for this function
     */
    public Function2(FunctionStyle style) {
        this.style = style;
    }

    /**
     * Returns the style for this function
     * @return  the style for this function
     */
    public FunctionStyle getStyle() {
        return style;
    }

    /**
     * Creates an BOOLEAN function that wraps to function provided
     * @param function  the function to wrap
     * @param <O>       the output type
     * @return          the newly created function wrapper
     */
    public static <O> Function2<Boolean,O> fromBoolean(BooleanFunction<O> function) {
        return new Function2<Boolean,O>(FunctionStyle.BOOLEAN) {
            @Override
            public final O apply(boolean input) {
                return function.apply(input);
            }
        };
    }

    /**
     * Creates an INTEGER function that wraps to function provided
     * @param function  the function to wrap
     * @param <O>       the output type
     * @return          the newly created function wrapper
     */
    public static <O> Function2<Integer,O> fromInteger(IntFunction<O> function) {
        return new Function2<Integer,O>(FunctionStyle.INTEGER) {
            @Override
            public final O apply(int input) {
                return function.apply(input);
            }
        };
    }

    /**
     * Creates an LONG function that wraps to function provided
     * @param function  the function to wrap
     * @param <O>       the output type
     * @return          the newly created function wrapper
     */
    public static <O> Function2<Long,O> fromLong(LongFunction<O> function) {
        return new Function2<Long,O>(FunctionStyle.LONG) {
            @Override
            public final O apply(long input) {
                return function.apply(input);
            }
        };
    }

    /**
     * Creates an LONG function that wraps to function provided
     * @param function  the function to wrap
     * @param <O>       the output type
     * @return          the newly created function wrapper
     */
    public static <O> Function2<Double,O> fromDouble(DoubleFunction<O> function) {
        return new Function2<Double,O>(FunctionStyle.DOUBLE) {
            @Override
            public final O apply(double input) {
                return function.apply(input);
            }
        };
    }

    /**
     * Applies this function to the given argument.
     * @param input the function argument
     * @return the function result
     */
    public O apply(boolean input) {
        throw new UnsupportedOperationException("This function is not of BOOLEAN style, rather: " + style);
    }

    /**
     * Applies this function to the given argument.
     * @param input the function argument
     * @return the function result
     */
    public O apply(int input) {
        throw new UnsupportedOperationException("This function is not of INTEGER style, rather: " + style);
    }

    /**
     * Applies this function to the given argument.
     * @param input the function argument
     * @return the function result
     */
    public O apply(long input) {
        throw new UnsupportedOperationException("This function is not of LONG style, rather: " + style);
    }

    /**
     * Applies this function to the given argument.
     * @param input the function argument
     * @return the function result
     */
    public O apply(double input) {
        throw new UnsupportedOperationException("This function is not of DOUBLE style, rather: " + style);
    }

    @Override
    public O apply(I input) {
        switch (style) {
            case BOOLEAN:   return apply(input != null ? (Boolean)input : false);
            case INTEGER:   return apply(input != null ? ((Number)input).intValue() : 0);
            case LONG:      return apply(input != null ? ((Number)input).longValue() : 0);
            case DOUBLE:    return apply(input != null ? ((Number)input).doubleValue() : 0);
            default: throw new UnsupportedOperationException("This mapper does not implement object conversion");
        }
    }
}
