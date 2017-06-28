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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * A utility class that provides some convenient functions to map contents of various Java Collections.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class Mapper {

    /**
     * A ListMapper interface that consumes a list value and it's index
     * @param <I>   the input type
     * @param <O>   the output type
     */
    @FunctionalInterface
    public interface ListMapper<I,O> {

        /**
         * Returns some output value given some entry in a list
         * @param index     the index of entry in a list
         * @param value     the value entry in list
         * @return          the resulting value to map to
         */
        O apply(int index, I value);
    }


    /**
     * Returns a new list which is the result of applying the mapping function to values of the input list
     * @param list          the input list on which to apply the mapping function to all elements
     * @param parallel      true if parallel mapping should be used
     * @param listMapper    the list mapper function to apply to all values in input list
     * @param <I>           the input list type
     * @param <O>           the output list type
     * @return              the output list
     */
    public static <I,O> List<O> apply(List<I> list, boolean parallel, ListMapper<I,O> listMapper) {
        final int size = list.size();
        final List<O> result = createList(list);
        IntStream.range(0, size).forEach(i -> result.add(null));
        final IntStream indexes = parallel ? IntStream.range(0, size).parallel() : IntStream.range(0, size);
        indexes.forEach(index -> {
            final I source = list.get(index);
            final O target = listMapper.apply(index, source);
            result.set(index, target);
        });
        return result;
    }



    /**
     * Returns a newly created list, ideally of the same type as input list
     * @param list      the basis list
     * @param <I>       the input type
     * @param <O>       the output type
     * @return          the newly created list
     */
    private static <I,O> List<O> createList(List<I> list) {
        if (list instanceof LinkedList) {
            return new LinkedList<>();
        } else {
            return new ArrayList<>(list.size());
        }
    }
}
