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
package com.zavtech.morpheus.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.range.Range;

/**
 * A convenience class to generate various kinds of Predicates.
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public class Predicates {

    /**
     * Returns a predicate representing an IN clause
     * @param values    the values for predicate
     * @param <T>       the value type
     * @return          the newly created predicate
     */
    @SafeVarargs()
    public static <T> Predicate<T> in(T... values) {
        final Set<T> valueSet = new HashSet<>(Arrays.asList(values));
        return valueSet::contains;
    }

    /**
     * Returns a predicate representing an IN clause
     * @param values    the values for predicate
     * @return          the newly created predicate
     */
    public static Predicate<Integer> in(int... values) {
        final Set<Integer> valueSet = IntStream.of(values).boxed().collect(Collectors.toSet());
        return valueSet::contains;
    }


    /**
     * Returns a predicate representing an IN clause
     * @param values    the values for predicate
     * @param <T>       the value type
     * @return          the newly created predicate
     */
    public static <T> Predicate<T> in(Iterable<T> values) {
        if (values instanceof Array) {
            final Array<T> array = (Array<T>)values;
            final Index<T> index = Index.of(array);
            return index::contains;
        } else if (values instanceof Range) {
            final Range<T> range = (Range<T>)values;
            final Array<T> array = range.toArray();
            final Index<T> index = Index.of(array);
            return index::contains;
        } else if (values instanceof Index) {
            final Index<T> index = (Index<T>)values;
            return index::contains;
        } else {
            final Set<T> valueSet = new HashSet<>();
            for (T value : values) valueSet.add(value);
            return valueSet::contains;
        }
    }

    /**
     * Returns a predicate representing an IN clause
     * @param values    the values for predicate
     * @param <T>       the value type
     * @return          the newly created predicate
     */
    public static <T> Predicate<T> in(Collection<T> values) {
        if (values instanceof Set) {
            final Set<T> valueSet = (Set<T>)values;
            return valueSet::contains;
        } else {
            final Set<T> valueSet = new HashSet<>(values);
            return valueSet::contains;
        }
    }

}
