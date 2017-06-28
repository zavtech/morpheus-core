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

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.stats.StatType;
import com.zavtech.morpheus.util.Tuple;

/**
 * An interface that provides various functions to operate on the rows or columns of a DataFrame
 *
 * @param <X>   the key type for this dimension
 * @param <Y>   the other dimension key type
 * @param <R>   the key type of the row dimension
 * @param <C>   the key type of the column dimension
 * @param <V>   the vector type for this dimension
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameAxis<X,Y,R,C,V extends DataFrameVector,T extends DataFrameAxis,G> extends Iterable<V> {

    enum Type {

        ROWS,
        COLS;

        public boolean isRow() {
            return this == ROWS;
        }

        public boolean isCol() {
            return this == COLS;
        }
    }

    /**
     * Returns the number of entries in this axis
     * @return  the count for this axis
     */
    int count();

    /**
     * Returns a parallel implementation of this interface
     * @return  a parallel implementation of this interface
     */
    T parallel();

    /**
     * Returns a sequential implementation of this interface
     * @return  a sequential implementation of this interface
     */
    T sequential();

    /**
     * Returns the key type for this dimension
     * @return      the key type for dimension
     */
    Class<X> keyType();

    /**
     * Returns a stream of row or column keys
     * @return  a stream of row or column keys
     */
    Stream<X> keys();

    /**
     * Returns the keys for this dimension as array
     * @return  array of keys for this dimension
     */
    Array<X> keyArray();

    /**
     * Returns a stream of the row or column ordinals for this axis.
     * @return  the stream of row or column ordinals
     */
    IntStream ordinals();

    /**
     * Returns true if this axis is operating in parallel mode
     * @return  true if operating in parallel mode
     */
    boolean isParallel();

    /**
     * Returns the array type for the row or column vector
     * @param key       the row or column vector
     * @return          the array type definition
     */
    Class<?> type(X key);

    /**
     * Returns a stream of array types for the row or column vectors
     * @return  a stream of of types for the row or column vectors
     */
    Stream<Class<?>> types();

    /**
     * Returns the row or column key for the index specified
     * @param index the row or column index
     * @return      the matching row or column key
     */
    X key(int index);

    /**
     * Returns the row or column ordinal for the key specified
     * @param key   the row or column key
     * @return      the row or column ordinal, -1 if not match
     */
    int ordinalOf(X key);

    /**
     * Returns the row or column ordinal for the key specified
     * @param key       the row or column key
     * @param strict    if true, throw an exception if no match for key
     * @return          the row or column ordinal, -1 if not match
     */
    int ordinalOf(X key, boolean strict);

    /**
     * Returns true if this dimension contains the key specified
     * @param key       the row or column key
     * @return          true if key is matched
     */
    boolean contains(X key);

    /**
     * Returns true if this dimension contains all the keys specified
     * @param keys  the row or column keys to match
     * @return      true if all keys are matched
     */
    boolean containsAll(Iterable<X> keys);

    /**
     * Returns a filtered axis operator based on the keys specified
     * @param keys      the keys to filter on
     * @return          the filtered row operator
     */
    T filter(X... keys);

    /**
     * Returns a filtered axis operator based on the keys specified
     * @param keys      the keys to filter on
     * @return          the filtered row operator
     */
    T filter(Iterable<X> keys);

    /**
     * Returns a filtered axis operator based on the keys specified
     * @param predicate the predicate to filter on
     * @return          the filtered row operator
     */
    T filter(Predicate<V> predicate);

    /**
     * Groups the DataFrame along this axis based on values in the vector specified
     * @param keys  the keys of the vectors to group by
     * @return      the resulting groups of DataFrames
     */
    G groupBy(Y... keys);

    /**
     * Groups rows according to the provided function and returns the groups
     * @param function  the grouping function to apply
     * @return          the resulting groups of DataFrames
     */
    G groupBy(Function<V,Tuple> function);

    /**
     * Returns a stream of vectors for this dimension
     * @return  the stream of vectors for this dimension
     */
    Stream<V> stream();

    /**
     * Returns an iterator of vectors for this dimension
     * @return  iterator of vectors
     */
    Iterator<V> iterator();

    /**
     * Returns an <code>Optional</code> on the first key in this dimension
     * @return  <code>Optional</code> on the first key (row or column key)
     */
    Optional<X> firstKey();

    /**
     * Returns an <code>Optional</code> on the last key in this dimension
     * @return  <code>Optional</code> on the last key (row or column key)
     */
    Optional<X> lastKey();

    /**
     * Returns an <code>Optional</code> on the first record in this dimension
     * @return  <code>Optional</code> on the first record in this dimension
     */
    Optional<V> first();

    /**
     * Returns an <code>Optional</code> on the last record in this dimension
     * @return  <code>Optional</code> on the last record in this dimension
     */
    Optional<V> last();

    /**
     * Returns an <code>Optional</code> on the first row that matches the predicate
     * @param predicate     the predicate to match rows
     * @return              <code>Optional</code> on the first row to match
     */
    Optional<V> first(Predicate<V> predicate);

    /**
     * Returns an <code>Optional</code> on the last row that matches the predicate
     * @param predicate     the predicate to match rows
     * @return              <code>Optional</code> on the last row to match
     */
    Optional<V> last(Predicate<V> predicate);

    /**
     * Returns an <code>Optional</code> on the greatest key strictly less than the given key
     * This operation only works if the index is sorted, otherwise result is undefined
     * @param key   the key to find the next lower key
     * @return  the <code>Optional</code> on the greatest key strictly less than the given key
     */
    Optional<X> lowerKey(X key);

    /**
     * Returns an <code>Optional</code> on the least key strictly greater than the given key
     * This operation only works if the index is sorted, otherwise result is undefined
     * @param key   the key to find the next highest from
     * @return      the <code>Optional</code> on the least key strictly greater than the given key
     */
    Optional<X> higherKey(X key);

    /**
     * Replaces an existing key with the new key in place
     * @param key       the existing key to replace
     * @param newKey    the replacement key, which must not already exist
     * @return          the DataFrame reference
     */
    DataFrame<R,C> replaceKey(X key, X newKey);

    /**
     * Returns a DataFrame view containing only the row or column keys specified
     * @param keys  the row or column keys to select
     * @return      the <code>DataFrame</code> view
     */
    DataFrame<R,C> select(X... keys);

    /**
     * Returns a DataFrame view containing only the row or column keys specified
     * @param keys  the row or column keys to select
     * @return      the <code>DataFrame</code> view
     */
    DataFrame<R,C> select(Iterable<X> keys);

    /**
     * Returns a DataFrame which includes all rows that match the specified predicate
     * @param predicate     the predicate to select matching rows
     * @return              the <code>DataFrame</code> view
     */
    DataFrame<R,C> select(Predicate<V> predicate);

    /**
     * Sorts the DataFrame along this axis based on the keys in ascending/descending order
     * @param ascending     true for ascending order, false for descending
     * @return              the sorted DataFrame
     */
    DataFrame<R,C> sort(boolean ascending);

    /**
     * Sorts the DataFrame along this axis in ascending/descending order
     * @param ascending     true for ascending, false for descending
     * @param key           the row or column key to sort by
     * @return              the sorted DataFrame
     */
    DataFrame<R,C> sort(boolean ascending, Y key);

    /**
     * Sorts the DataFrame along this axis in ascending/descending order
     * @param ascending     true for ascending, false for descending
     * @param keys          the list of row or column keys to sort by
     * @return              the sorted DataFrame
     */
    DataFrame<R,C> sort(boolean ascending, List<Y> keys);

    /**
     * Sorts the DataFrame along this axis according to the comparator provided
     * @param comparator    the comparator to sort rows, null to remove sorting
     * @return              the sorted DataFrame
     */
    DataFrame<R,C> sort(Comparator<V> comparator);

    /**
     * Subtracts the mean from each row / column defined by this axis
     * @param inPlace   if true, demean in pliace
     * @return          the resulting DataFrame
     */
    DataFrame<R,C> demean(boolean inPlace);

    /**
     * Returns the row/column that represents the minimum value given the comparator
     * @param comparator    the comparator to compare rows/columns
     * @return              the row/column, empty if size() == 0
     */
    Optional<V> min(Comparator<V> comparator);

    /**
     * Returns the row/column that represents the maximum value given the comparator
     * @param comparator    the comparator to compare rows/columns
     * @return              the row/column, empty if size() == 0
     */
    Optional<V> max(Comparator<V> comparator);

    /**
     * Adds all the rows or columns from the frame provided
     * Only data for intersecting rows will be copied into this frame
     * @param frame     the frame to add columns from
     * @return          the column keys of newly added columns
     */
    Array<X> addAll(DataFrame<R,C> frame);

}
