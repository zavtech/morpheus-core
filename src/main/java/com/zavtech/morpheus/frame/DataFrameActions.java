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
package com.zavtech.morpheus.frame;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import com.zavtech.morpheus.util.functions.ToBooleanFunction;

/**
 * An interface to common actions that can be applied to a DataFrame and its affiliated structures.
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
interface DataFrameActions<R,C,T> {

    /**
     * Iterates over all values in the underlying data structure
     * @param consumer  the consumer that accepts each value
     */
    T forEachValue(Consumer<DataFrameValue<R,C>> consumer);

    /**
     * Applies a mapping function to all values in the underlying data structure
     * @param mapper    the mapper function to apply
     * @return          this entity
     */
    T applyBooleans(ToBooleanFunction<DataFrameValue<R,C>> mapper);

    /**
     * Applies a mapping function to all values in the underlying data structure
     * @param mapper    the mapper function to apply
     * @return          this entity
     */
    T applyInts(ToIntFunction<DataFrameValue<R,C>> mapper);

    /**
     * Applies a mapping function to all values in the underlying data structure
     * @param mapper    the mapper function to apply
     * @return          this entity
     */
    T applyLongs(ToLongFunction<DataFrameValue<R,C>> mapper);

    /**
     * Applies a mapping function to all values in the underlying data structure
     * @param mapper    the mapper function to apply
     * @return          this entity
     */
    T applyDoubles(ToDoubleFunction<DataFrameValue<R,C>> mapper);

    /**
     * Applies a mapping function to all values in the underlying data structure
     * @param mapper    the mapper function to apply
     * @return          this entity
     */
    T applyValues(Function<DataFrameValue<R,C>,?> mapper);

}
