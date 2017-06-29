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
package com.zavtech.morpheus.array;

import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * A class that provides access to type specific Collectors for collecting data in Morpheus arrays using Java 8 stream API.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class ArrayCollector {


    private static final int DEFAULT_INITIAL_CAPACITY = 1000;

    /**
     * Returns a collector that collects integer values in a Morpheus array
     * @return  the newly created collector
     */
    public static Collector<Integer,ArrayBuilder<Integer>,Array<Integer>> ofInts() {
        return ArrayCollector.of(Integer.class, DEFAULT_INITIAL_CAPACITY);
    }


    /**
     * Returns a collector that collects integer values in a Morpheus array
     * @param initialCapacity   the initial capacity for array
     * @return  the newly created collector
     */
    public static Collector<Integer,ArrayBuilder<Integer>,Array<Integer>> ofInts(int initialCapacity) {
        return ArrayCollector.of(Integer.class, initialCapacity);
    }

    /**
     * Returns a collector that collects items in a Morpheus array
     * @param type              the array type
     * @param expectedLength    an estimate of the expected length, does not have to be exact
     * @param <T>               the array element type
     * @return                  the newly created collector
     */
    public static <T> Collector<T,ArrayBuilder<T>,Array<T>> of(Class<T> type, int expectedLength) {
        final Supplier<ArrayBuilder<T>> supplier = () -> ArrayBuilder.of(expectedLength, type);
        final BinaryOperator<ArrayBuilder<T>> combiner = ArrayBuilder::addAll;
        final BiConsumer<ArrayBuilder<T>,T> accumulator = ArrayBuilder::add;
        final Function<ArrayBuilder<T>,Array<T>> finisher = ArrayBuilder::toArray;
        return Collector.of(supplier, accumulator, combiner, finisher);
    }

}
