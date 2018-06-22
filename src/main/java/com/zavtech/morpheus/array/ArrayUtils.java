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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.range.Range;

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

/**
 * A utility class that provides various useful functions to operate on Morpheus arrays
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class ArrayUtils {

    /**
     * Returns a collector that collects items in a Morpheus array
     * @return                  the newly created collector
     */
    @SuppressWarnings("unchecked")
    public static <T> Collector<T,ArrayBuilder<T>,Array<T>> toArray() {
        return ArrayUtils.toArray(null, 1000);
    }

    /**
     * Returns a collector that collects items in a Morpheus array
     * @param expectedLength    an estimate of the expected length, does not have to be exact
     * @return                  the newly created collector
     */
    @SuppressWarnings("unchecked")
    public static <T> Collector<T,ArrayBuilder<T>,Array<T>> toArray(int expectedLength) {
        return ArrayUtils.toArray(null, expectedLength);
    }

    /**
     * Returns a collector that collects items in a Morpheus array
     * @param type              the array type
     * @param expectedLength    an estimate of the expected length, does not have to be exact
     * @param <T>               the array element type
     * @return                  the newly created collector
     */
    public static <T> Collector<T,ArrayBuilder<T>,Array<T>> toArray(Class<T> type, int expectedLength) {
        final Supplier<ArrayBuilder<T>> supplier = () -> ArrayBuilder.of(expectedLength, type);
        final BinaryOperator<ArrayBuilder<T>> combiner = ArrayBuilder::addAll;
        final BiConsumer<ArrayBuilder<T>,T> accumulator = ArrayBuilder::add;
        final Function<ArrayBuilder<T>,Array<T>> finisher = ArrayBuilder::toArray;
        return Collector.of(supplier, accumulator, combiner, finisher);
    }


    /**
     * Creates an Array from the values in the Iterable specified
     * @param values    the iterable collection of values
     * @return          the array containing values from the iterable
     */
    @SuppressWarnings("unchecked")
    public static <V> Array<V> toArray(Iterable<V> values) {
        if (values instanceof Array) {
            return (Array<V>)values;
        } else if (values instanceof Index) {
            return ((Index<V>) values).toArray();
        } else if (values instanceof Range) {
            return ((Range<V>) values).toArray();
        } else if (values instanceof Collection) {
            final ArrayBuilder<V> builder = ArrayBuilder.of(((Collection)values).size());
            return builder.addAll(values).toArray();
        } else {
            final ArrayBuilder<V> builder = ArrayBuilder.of(1000);
            return builder.addAll(values).toArray();
        }
    }


    /**
     * Returns an array of distinct values from the stream in the order they were observed
     * @param values    the stream to extract distinct values from
     * @param limit     the max number of distinct values to capture
     * @return          the array of distinct values in the order they were observed
     */
    public static Array<Integer> distinct(IntStream values, int limit) {
        final int capacity = limit < it.unimi.dsi.fastutil.Arrays.MAX_ARRAY_SIZE ? limit : 1000;
        final DistinctInts distinct = new DistinctInts(limit, capacity);
        final PrimitiveIterator.OfInt iterator = values.iterator();
        while (iterator.hasNext()) {
            final int value = iterator.next();
            final boolean hitLimit = distinct.add(value);
            if (hitLimit) {
                break;
            }
        }
        return distinct.toArray();
    }


    /**
     * Returns an array of distinct values from the stream in the order they were observed
     * @param values    the stream to extract distinct values from
     * @param limit     the max number of distinct values to capture
     * @return          the array of distinct values in the order they were observed
     */
    public static Array<Long> distinct(LongStream values, int limit) {
        final int capacity = limit < it.unimi.dsi.fastutil.Arrays.MAX_ARRAY_SIZE ? limit : 1000;
        final DistinctLongs distinct = new DistinctLongs(limit, capacity);
        final PrimitiveIterator.OfLong iterator = values.iterator();
        while (iterator.hasNext()) {
            final long value = iterator.next();
            final boolean hitLimit = distinct.add(value);
            if (hitLimit) {
                break;
            }
        }
        return distinct.toArray();
    }


    /**
     * Returns an array of distinct values from the stream in the order they were observed
     * @param values    the stream to extract distinct values from
     * @param limit     the max number of distinct values to capture
     * @return          the array of distinct values in the order they were observed
     */
    public static Array<Double> distinct(DoubleStream values, int limit) {
        final int capacity = limit < it.unimi.dsi.fastutil.Arrays.MAX_ARRAY_SIZE ? limit : 1000;
        final DistinctDoubles distinct = new DistinctDoubles(limit, capacity);
        final PrimitiveIterator.OfDouble iterator = values.iterator();
        while (iterator.hasNext()) {
            final double value = iterator.next();
            final boolean hitLimit = distinct.add(value);
            if (hitLimit) {
                break;
            }
        }
        return distinct.toArray();
    }


    /**
     * Returns an array of distinct values from the stream in the order they were observed
     * @param values    the stream to extract distinct values from
     * @param limit     the max number of distinct values to capture
     * @return          the array of distinct values in the order they were observed
     */
    public static <V> Array<V> distinct(Stream<V> values, int limit) {
        final int capacity = limit < it.unimi.dsi.fastutil.Arrays.MAX_ARRAY_SIZE ? limit : 1000;
        final DistinctValues<V> distinct = new DistinctValues<>(limit, capacity);
        final Iterator<V> iterator = values.iterator();
        while (iterator.hasNext()) {
            final V value = iterator.next();
            final boolean hitLimit = distinct.add(value);
            if (hitLimit) {
                break;
            }
        }
        return distinct.toArray();
    }


    /**
     * A convenience base class for distinct value calculators
     * @param <T>   the data type
     */
    private static class DistinctCalculator<T> {

        protected ArrayBuilder<T> builder;

        /**
         * Constructor
         * @param type      the data type
         * @param capacity  the initial capacity for array builder
         */
        DistinctCalculator(Class<T> type, int capacity) {
            this.builder = ArrayBuilder.of(capacity, type);
        }

        /**
         * Returns the array of distinct values in the order they were observed
         * @return  the array of distinct values in the order they were observed
         */
        public final Array<T> toArray() {
            return builder.toArray();
        }
    }

    /**
     * Captures distinct int values using an efficient Trove set.
     */
    private static class DistinctInts extends DistinctCalculator<Integer> {

        private int limit;
        private IntSet distinctSet;

        /**
         * Constructor
         * @param limit the limit for this calculator
         * @param capacity  the initial capacity for set
         */
        DistinctInts(int limit, int capacity) {
            super(Integer.class, capacity);
            this.limit = limit;
            this.distinctSet = new IntOpenHashSet(capacity);
        }

        /**
         * Adds an int observation and returns true if this calculator has reached limit
         * @param value the value to add
         * @return      true if this calculator has hit limit
         */
        public final boolean add(int value) {
            final boolean added = distinctSet.add(value);
            if (added) builder.addInt(value);
            return !(distinctSet.size() < limit);
        }
    }


    /**
     * Captures distinct long values using an efficient Trove set.
     */
    private static class DistinctLongs extends DistinctCalculator<Long> {

        private int limit;
        private LongSet distinctSet;

        /**
         * Constructor
         * @param limit the limit for this calculator
         */
        DistinctLongs(int limit, int capacity) {
            super(Long.class, capacity);
            this.limit = limit;
            this.distinctSet = new LongOpenHashSet(capacity);
        }

        /**
         * Adds an int observation and returns true if this calculator has reached limit
         * @param value the value to add
         * @return      true if this calculator has hit limit
         */
        public final boolean add(long value) {
            final boolean added = distinctSet.add(value);
            if (added) builder.addLong(value);
            return !(distinctSet.size() < limit);
        }
    }

    /**
     * Captures distinct double values using an efficient Trove set.
     */
    private static class DistinctDoubles extends DistinctCalculator<Double> {

        private int limit;
        private DoubleSet distinctSet;

        /**
         * Constructor
         * @param limit the limit for this calculator
         */
        DistinctDoubles(int limit, int capacity) {
            super(Double.class, capacity);
            this.limit = limit;
            this.distinctSet = new DoubleOpenHashSet(capacity);
        }

        /**
         * Adds an int observation and returns true if this calculator has reached limit
         * @param value the value to add
         * @return      true if this calculator has hit limit
         */
        public final boolean add(double value) {
            final boolean added = distinctSet.add(value);
            if (added) builder.addDouble(value);
            return !(distinctSet.size() < limit);
        }
    }

    /**
     * Captures distinct values using a Java set
     */
    private static class DistinctValues<V> extends DistinctCalculator<V> {

        private int limit;
        private Set<V> distinctSet;

        /**
         * Constructor
         * @param limit the limit for this calculator
         */
        @SuppressWarnings("unchecked")
        DistinctValues(int limit, int capacity) {
            super((Class<V>)Object.class, capacity);
            this.limit = limit;
            this.distinctSet = new HashSet<>(capacity);
        }

        /**
         * Adds an int observation and returns true if this calculator has reached limit
         * @param value the value to add
         * @return      true if this calculator has hit limit
         */
        public final boolean add(V value) {
            final boolean added = distinctSet.add(value);
            if (added) builder.add(value);
            return !(distinctSet.size() < limit);
        }
    }

}
