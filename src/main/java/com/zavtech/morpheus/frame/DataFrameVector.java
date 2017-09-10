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
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.stats.Statistic1;
import com.zavtech.morpheus.stats.Stats;
import com.zavtech.morpheus.util.Bounds;

/**
 * An interface to a row or column vector in a <code>DataFrame</code>
 *
 * <p>This is open source software released under the <a href="http://www.ap`ache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameVector<X,Y,R,C,Z> extends DataFrameActions<R,C,Z>, DataFrameIterators<R,C>, Iterable<DataFrameValue<R,C>> {

    /**
     * Returns the key for this vector
     * @return  the key for vector
     */
    X key();

    /**
     * Returns the number of elements in this vector
     * @return      the number of elements
     */
    int size();

    /**
     * Returns the ordinal of this vector in the frame
     * @return      the view ordinal for this vector
     */
    int ordinal();

    /**
     * Returns a parallel implementation of this vector
     * @return  parallel implementation of this vector
     */
    Z parallel();

    /**
     * Returns true if this vector represents a row
     * @return  true if a row
     */
    boolean isRow();

    /**
     * Returns true if this vector represents a column
     * @return  true if a column
     */
    boolean isColumn();

    /**
     * Returns true if this vector contains only numerical data
     * @return      true if vector contains only numerical data
     */
    boolean isNumeric();

    /**
     * Returns true if this vector is a parallel implementation
     * @return  true if parallel implementation
     */
    boolean isParallel();

    /**
     * Returns true if any of the values in this vector is null
     * @return  true if any value in this vector is null
     */
    boolean hasNulls();

    /**
     * Returns the row keys for this vector, one key if this is a row
     * @return  the stream of row keys for vector
     */
    Stream<R> rowKeys();

    /**
     * Returns the column keys for this vector, one key if this is a column
     * @return  the stream of column keys for vector
     */
    Stream<C> colKeys();

    /**
     * Returns a reference to the frame to which this vector belongs
     * @return  the frame to which this vector belongs
     */
    DataFrame<R,C> frame();

    /**
     * Returns the most specific type that describes elements of this vector
     * @return      the most specific type that describes elements of this vector
     */
    Class<?> typeInfo();

    /**
     * Returns the ranks of the values in this vector
     * @return      the rank of values in this vector
     */
    DataFrame<R,C> rank();

    /**
     * Returns an array of distinct values for this vector in the order they appear
     * @param <V>   the type for value
     * @return      the array of distinct values in this vector
     */
    <V> Array<V> distinct();

    /**
     * Returns an array of distinct values for this vector in the order they appear limited to the amount specified
     * @param limit the max number of distinct items to return
     * @param <V>   the type for value
     * @return      the array of distinct values in this vector
     */
    <V> Array<V> distinct(int limit);

    /**
     * Moves this vector so it points at the location with the specified key
     * @param key   the vector key to move to
     * @return      this row reference
     */
    Z moveTo(X key);

    /**
     * Moves this vector so it points at the location with the specified ordinal
     * @param ordinal   the vector ordinal to move to
     * @return          this row reference
     */
    Z moveTo(int ordinal);

    /**
     * Returns a stream of primitive integers
     * @return  the stream of primitive integers
     */
    IntStream toIntStream();

    /**
     * Returns a stream of primitive longs
     * @return  the stream of primitive longs
     */
    LongStream toLongStream();

    /**
     * Returns a stream of primitive doubles
     * @return  the stream of primitive doubles
     */
    DoubleStream toDoubleStream();

    /**
     * Returns a stream of values in this vector
     * @return  the stream of values
     */
    <V> Stream<V> toValueStream();

    /**
     * Returns an array of values for this vector
     * @param <V>   the array type
     * @return      the array of values
     */
    <V> Array<V> toArray();

    /**
     * Returns a newly created DataFrame representation of this vector
     * @return      the DataFrame representation of this vector
     */
    DataFrame<R,C> toDataFrame();

    /**
     * Returns a DataFrame representing a frquency distribution of this vector
     * @param binCount      the number of bins to include in the histogram frame
     * @return              the newly created DataFrame with frequency distribution
     */
    DataFrame<Double,String> hist(int binCount);

    /**
     * Returns the stats for this vector
     * @return      the stats for this vector
     */
    Stats<Double> stats();

    /**
     * Returns a stream of values for this vector
     * @return  the stream of values for this vector
     */
    Stream<DataFrameValue<R,C>> values();

    /**
     * Returns the bounds with the min/max values for this vector
     * @param <T>   the type for bounds
     * @return      the upper and lower bounds for vector, empty if no data
     */
    <T> Optional<Bounds<T>> bounds();

    /**
     * Returns the minimum value for this vector if one exists
     * @return      the minimum value, empty if no data
     */
    Optional<DataFrameValue<R,C>> min();

    /**
     * Returns the maximum value for this vector if one exists
     * @return      the maximum value, empty if no data
     */
    Optional<DataFrameValue<R,C>> max();

    /**
     * Returns the index of the min value in this vector
     * @param comparator    the comparator that defines order
     * @return              the index of min value
     */
    Optional<DataFrameValue<R,C>> min(Comparator<DataFrameValue<R,C>> comparator);

    /**
     * Returns the index of the max value in this vector
     * @param comparator    the comparator that defines order
     * @return              the index of max value
     */
    Optional<DataFrameValue<R,C>> max(Comparator<DataFrameValue<R,C>> comparator);

    /**
     * Returns the minimum value for this vector if one exists
     * @param predicate the predicate for conditional min
     * @return      the minimum value, empty if no data
     */
    Optional<DataFrameValue<R,C>> min(Predicate<DataFrameValue<R,C>> predicate);

    /**
     * Returns the maximum value for this vector if one exists
     * @param predicate the predicate for conditional max
     * @return      the maximum value, empty if no data
     */
    Optional<DataFrameValue<R,C>> max(Predicate<DataFrameValue<R,C>> predicate);

    /**
     * Finds the first value in this vector that matches the predicate
     * @param predicate the predicate to match values
     * @return          the first match, which could be None
     */
    Optional<DataFrameValue<R,C>> first(Predicate<DataFrameValue<R,C>> predicate);

    /**
     * Finds the last value in this vector that matches the predicate
     * @param predicate the predicate to match values
     * @return          the last match, which could be null
     */
    Optional<DataFrameValue<R,C>> last(Predicate<DataFrameValue<R,C>> predicate);

    /**
     * Searches this vector for the value using a binary search algorithm on the assumption the data is sorted
     * If the data is not ordered in a way that is sorted in ascending order, the result is undefined.
     * @param value     the value to search for in this vector
     * @param <T>       the type of the value to search for
     * @return          the DataFrameValue match from which keys and ordinals can be accessed.
     */
    <T> Optional<DataFrameValue<R,C>> binarySearch(T value);

    /**
     * Searches this vector for the value using a binary search algorithm on the assumption the data is sorted according to the comparator.
     * If the data is not ordered in a way that is sorted in ascending order according to the comparator, the result is undefined.
     * @param value         the value to search for in this vector
     * @param comparator    the comparator that defines ordering
     * @param <T>       the type of the value to search for
     * @return              the DataFrameValue match from which keys and ordinals can be accessed.
     */
    <T> Optional<DataFrameValue<R,C>> binarySearch(T value, Comparator<T> comparator);

    /**
     * Searches a subset of this vector for the value using a binary search algorithm on the assumption the data is sorted according to the comparator.
     * If the data is not ordered in a way that is sorted in ascending order according to the comparator, the result is undefined.
     * @param offset        the offset from the start of this vector
     * @param length        the number of items after offset to include
     * @param value         the value to search for in this vector
     * @param comparator    the comparator that defines ordering
     * @param <T>       the type of the value to search for
     * @return              the DataFrameValue match from which keys and ordinals can be accessed.
     */
    <T> Optional<DataFrameValue<R,C>> binarySearch(int offset, int length, T value, Comparator<T> comparator);

    /**
     * Computes a univariate statistic on this vector
     * @param statistic     the statistic to compute
     * @param offset        the offset from where to start in this vector
     * @param length        the number of items from offset to include
     * @return              the univariate statistic value
     */
    double compute(Statistic1 statistic, int offset, int length);

    /**
     * Returns the value in this vector for the key specified
     * @param key   the row or column key if this represents a column or row respectively
     * @return      the value for key
     */
    boolean getBoolean(Y key);

    /**
     * Returns the value in this vector for the ordinal specified
     * @param ordinal   the row or column ordinal if this represents a column or row respectively
     * @return      the value for key
     */
    boolean getBoolean(int ordinal);

    /**
     * Returns the value in this vector for the key specified
     * @param key   the row or column key if this represents a column or row respectively
     * @return      the value for key
     */
    int getInt(Y key);

    /**
     * Returns the value in this vector for the ordinal specified
     * @param ordinal   the row or column ordinal if this represents a column or row respectively
     * @return      the value for key
     */
    int getInt(int ordinal);

    /**
     * Returns the value in this vector for the key specified
     * @param key   the row or column key if this represents a column or row respectively
     * @return      the value for key
     */
    long getLong(Y key);

    /**
     * Returns the value in this vector for the ordinal specified
     * @param ordinal   the row or column ordinal if this represents a column or row respectively
     * @return      the value for key
     */
    long getLong(int ordinal);

    /**
     * Returns the value in this vector for the key specified
     * @param key   the row or column key if this represents a column or row respectively
     * @return      the value for key
     */
    double getDouble(Y key);

    /**
     * Returns the value in this vector for the ordinal specified
     * @param ordinal   the row or column ordinal if this represents a column or row respectively
     * @return      the value for key
     */
    double getDouble(int ordinal);

    /**
     * Returns the value in this vector for the key specified
     * @param key   the row or column key if this represents a column or row respectively
     * @return      the value for key
     */
    <V> V getValue(Y key);

    /**
     * Returns the value in this vector for the ordinal specified
     * @param ordinal   the row or column ordinal if this represents a column or row respectively
     * @return      the value for key
     */
    <V> V getValue(int ordinal);

    /**
     * Sets the value in this vector for the key specified
     * @param key   the row or column key if this represents a column or row respectively
     * @param value the value to set
     * @return      the previous value
     */
    boolean setBoolean(Y key, boolean value);

    /**
     * Sets the value in this vector for the ordinal specified
     * @param ordinal   the row or column ordinal if this represents a column or row respectively
     * @param value     the value to set
     * @return          the previous value
     */
    boolean setBoolean(int ordinal, boolean value);

    /**
     * Sets the value in this vector for the key specified
     * @param key   the row or column key if this represents a column or row respectively
     * @param value the value to set
     * @return      the previous value
     */
    int setInt(Y key, int value);

    /**
     * Sets the value in this vector for the ordinal specified
     * @param ordinal   the row or column ordinal if this represents a column or row respectively
     * @param value     the value to set
     * @return          the previous value
     */
    int setInt(int ordinal, int value);

    /**
     * Sets the value in this vector for the key specified
     * @param key   the row or column key if this represents a column or row respectively
     * @param value the value to set
     * @return      the previous value
     */
    long setLong(Y key, long value);

    /**
     * Sets the value in this vector for the ordinal specified
     * @param ordinal   the row or column ordinal if this represents a column or row respectively
     * @param value     the value to set
     * @return          the previous value
     */
    long setLong(int ordinal, long value);

    /**
     * Sets the value in this vector for the key specified
     * @param key   the row or column key if this represents a column or row respectively
     * @param value the value to set
     * @return      the previous value
     */
    double setDouble(Y key, double value);

    /**
     * Sets the value in this vector for the ordinal specified
     * @param ordinal   the row or column ordinal if this represents a column or row respectively
     * @param value     the value to set
     * @return          the previous value
     */
    double setDouble(int ordinal, double value);

    /**
     * Sets the value in this vector for the key specified
     * @param key   the row or column key if this represents a column or row respectively
     * @param value the value to set
     * @return      the previous value
     */
    <V> V setValue(Y key, V value);

    /**
     * Sets the value in this vector for the ordinal specified
     * @param ordinal   the row or column ordinal if this represents a column or row respectively
     * @param value     the value to set
     * @return          the previous value
     */
    <V> V setValue(int ordinal, V value);

}
