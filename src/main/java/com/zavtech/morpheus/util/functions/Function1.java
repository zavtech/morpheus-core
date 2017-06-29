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

import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

/**
 * A Mapper that exposes various methods that produce primitive types or can be generalized to a standard java.util.function.Function
 *
 * In Java 8 various functional interfaces were introduced, such as java.util.function.Function, ToIntFunction, ToDoubleFunction and
 * so on. The problem is that these interfaces do not have a common super type, so you cannot write generalized code that can operation
 * on these functions and also avoid boxing in cases where primitives are involved. This Mapper class attempts to solve that by providing
 * a wrapper around all these type specific functions. This makes it possible to write a generalized API that can avoid boxing when
 * necessary. For example:
 *
 * <pre>
 *      public void doSomething(Mapper<String,?> mapper) {
 *          switch (mapper.getType()) {
 *              case INTEGER:  mapper.toInt();
 *              case DOUBLE:   mapper.toDouble();
 *              case LONG:     mapper.toLong();
 *          }
 *      }
 * </pre>
 *
 * @param <I>   the input type
 * @param <O>   the output type
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public class Function1<I,O> implements Function<I,O> {

    private static final Integer    ZERO_INT = 0;
    private static final Long       ZERO_LONG = 0L;
    private static final Double     ZERO_DOUBLE = 0d;
    private static final Double     NAN_DOUBLE = Double.NaN;

    private FunctionStyle style;

    /**
     * Constructor
     * @param style  the style for this function
     */
    protected Function1(FunctionStyle style) {
        this.style = style;
    }

    /**
     * Returns the type for this mapper
     * @return  the type for this mapper
     */
    public FunctionStyle getStyle() {
        return style;
    }

    /**
     * Creates an BOOLEAN mapper that wraps to function provided
     * @param function  the function to wrap
     * @param <I>       the input type
     * @return          the newly created mapper
     */
    public static <I> Function1<I,Boolean> toBoolean(ToBooleanFunction<I> function) {
        return new Function1<I,Boolean>(FunctionStyle.BOOLEAN) {
            @Override
            public final boolean applyAsBoolean(I value) {
                return function.applyAsBoolean(value);
            }
        };
    }

    /**
     * Creates an INTEGER mapper that wraps to function provided
     * @param function  the function to wrap
     * @param <I>       the input type
     * @return          the newly created mapper
     */
    public static <I,O> Function1<I,Integer> toInt(ToIntFunction<I> function) {
        return new Function1<I,Integer>(FunctionStyle.INTEGER) {
            @Override
            public final int applyAsInt(I value) {
                return function.applyAsInt(value);
            }
        };
    }

    /**
     * Creates an LONG mapper that wraps to function provided
     * @param function  the function to wrap
     * @param <I>       the input type
     * @return          the newly created mapper
     */
    public static <I,O> Function1<I,Long> toLong(ToLongFunction<I> function) {
        return new Function1<I,Long>(FunctionStyle.LONG) {
            @Override
            public final long applyAsLong(I value) {
                return function.applyAsLong(value);
            }
        };
    }

    /**
     * Creates an DOUBLE mapper that wraps to function provided
     * @param function  the function to wrap
     * @param <I>       the input type
     * @return          the newly created mapper
     */
    public static <I,O> Function1<I,Double> toDouble(ToDoubleFunction<I> function) {
        return new Function1<I,Double>(FunctionStyle.DOUBLE) {
            @Override
            public final double applyAsDouble(I value) {
                return function.applyAsDouble(value);
            }
        };
    }

    /**
     * Creates an OBJECT mapper that wraps to function provided
     * @param function  the function to wrap
     * @param <I>       the input type
     * @param <O>       the output type
     * @return          the newly created mapper
     */
    public static <I,O> Function1<I,O> toValue(Function<I,O> function) {
        return new Function1<I,O>(FunctionStyle.OBJECT) {
            @Override
            public final O apply(I value) {
                return function.apply(value);
            }
        };
    }

    /**
     * Transforms some input value into a primitive boolean
     * @param value     the value to transform
     * @return          the primitive boolean
     */
    public boolean applyAsBoolean(I value) {
        throw new UnsupportedOperationException("This function is not of BOOLEAN style, rather: " + style);
    }

    /**
     * Transforms some input value into a primitive int
     * @param value     the value to transform
     * @return          the primitive int
     */
    public int applyAsInt(I value) {
        throw new UnsupportedOperationException("This function is not of INTEGER style, rather: " + style);
    }

    /**
     * Transforms some input value into a primitive long
     * @param value     the value to transform
     * @return          the primitive long
     */
    public long applyAsLong(I value) {
        throw new UnsupportedOperationException("This function is not of LONG style, rather: " + style);
    }

    /**
     * Transforms some input value into a primitive double
     * @param value     the value to transform
     * @return          the primitive double
     */
    public double applyAsDouble(I value) {
        throw new UnsupportedOperationException("This function is not of DOUBLE style, rather: " + style);
    }

    @Override()
    @SuppressWarnings("unchecked")
    public O apply(I value) {
        if (style == FunctionStyle.BOOLEAN) {
            return applyAsBoolean(value) ? (O)Boolean.TRUE : (O)Boolean.FALSE;
        } else if (style == FunctionStyle.INTEGER) {
            final int intValue = applyAsInt(value);
            return intValue == 0 ? (O)ZERO_INT : (O)new Integer(intValue);
        } else if (style == FunctionStyle.LONG) {
            final long longValue = applyAsLong(value);
            return longValue == 0L ? (O)ZERO_LONG : (O)new Long(longValue);
        } else if (style == FunctionStyle.DOUBLE) {
            final double doubleValue = applyAsDouble(value);
            return Double.isNaN(doubleValue) ? (O)NAN_DOUBLE : doubleValue == 0d ? (O)ZERO_DOUBLE : (O)new Double(doubleValue);
        } else {
            throw new UnsupportedOperationException("This function is not implemented: " + style);
        }
    }

}
