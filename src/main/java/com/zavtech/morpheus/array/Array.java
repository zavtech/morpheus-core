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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import com.zavtech.morpheus.stats.Stats;
import com.zavtech.morpheus.util.functions.BooleanConsumer;
import com.zavtech.morpheus.util.Bounds;
import com.zavtech.morpheus.util.functions.ToBooleanFunction;

/**
 * An generic interface to an Array designed to hold type specific content as efficiently as possible.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public interface Array<T> extends Iterable<T>, Serializable, Cloneable {

    /**
     * Returns the length for this array
     * @return  the length for this array
     */
    int length();

    /**
     * Returns the default value for this array
     * @return  the default value for array
     */
    T defaultValue();

    /**
     * Returns the load factor for this array, 1 implies dense, < 1 implies sparse
     * @return  the load factor for this array
     */
    float loadFactor();

    /**
     * Returns the element class for this array
     * @return  the element class for array
     */
    Class<T> type();

    /**
     * Returns the type code for this array
     * @return  the array type code
     */
    ArrayType typeCode();

    /**
     * Returns the style for this array (DENSE, SPARSE, MAPPED)
     * @return  the storage storage style for this array
     */
    ArrayStyle style();

    /**
     * Returns true if this is a read-only wrapper of an array
     * @return  true if this is a read only array
     */
    boolean isReadOnly();

    /**
     * Returns true if this is a parallel array
     * @return  true if this is a parallel array
     */
    boolean isParallel();

    /**
     * Returns a parallel version of this array
     * @return  parallel version of this array
     */
    Array<T> parallel();

    /**
     * Returns a sequential version of this array
     * @return  sequential version of this array
     */
    Array<T> sequential();

    /**
     * Returns a read-only view of this array
     * @return  a ready only view of this array
     */
    Array<T> readOnly();

    /**
     * Returns a deep copy of this array
     * @return  a deep copy of this array
     */
    Array<T> copy();

    /**
     * Returns a deep copy of this array including only the values at the indexes specified
     * @param indexes   the indexes of values to include in the copy
     * @return          the deep copy of a subset of this array
     */
    Array<T> copy(int[] indexes);

    /**
     * Returns a deep copy of a subset of this array including only the values in the range
     * @param start     the start index of range, inclusive
     * @param end       the end of the range, exclusive
     * @return          the deep copy of a subset of this array
     */
    Array<T> copy(int start, int end);

    /**
     * Returns a newly created cursor over this array
     * @return  a newly created cursor over this array
     */
    ArrayCursor<T> cursor();

    /**
     * Concatenates some other array onto this array
     * @param other the other array to concatenate onto this
     * @return      a newly created array of this + other
     */
    Array<T> concat(Array<T> other);

    /**
     * Returns a newly created array containing distinct values from this array
     * @return  the Array of distinct values in the order they appear in this array
     */
    Array<T> distinct();

    /**
     * Returns a newly created array containing distinct values from this array
     * @param limit the max number of distinct values to include
     * @return  the Array of distinct values in the order they appear in this array
     */
    Array<T> distinct(int limit);

    /**
     * Returns an light-weight unmodifiable List view over this array
     * @return  a light-weight unmodifiable List view on this array
     */
    List<T> toList();

    /**
     * Returns the data streaming interface to this array
     * @return  the data streaming interface to this array
     */
    ArrayStreams<T> stream();

    /**
     * Returns the data streaming interface to this array, including only the range specified
     * @param start     the start index of range, inclusive
     * @param end       the end of the range, exclusive
     * @return  the data streaming interface to this array
     */
    ArrayStreams<T> stream(int start, int end);

    /**
     * Returns the minimum value in the array
     * This requires the elements to be Comparable
     * @return  the min value in the array
     */
    Optional<T> min();

    /**
     * Returns the maximum value in the array
     * This requires the elements to be Comparable
     * @return  the max value in the array
     */
    Optional<T> max();

    /**
     * Returns the upper and lower bounds of this array
     * This requires that the elements of this Array are Comparable
     * @return      the bounds for array, empty if array length is zero or all values are null
     */
    Optional<Bounds<T>> bounds();

    /**
     * Returns the first value that matches the predicate specified
     * @param predicate     the predicate to match values
     * @return      the first match to predicate, Optional.empty() if no match
     */
    Optional<ArrayValue<T>> first(Predicate<ArrayValue<T>> predicate);

    /**
     * Returns the last value that matches the predicate specified
     * @param predicate     the predicate to match values
     * @return      the last match to predicate, Optional.empty() if no match
     */
    Optional<ArrayValue<T>> last(Predicate<ArrayValue<T>> predicate);

    /**
     * Returns the greatest value strictly less than the given value
     * This operation only works if the array is sorted, otherwise result is undefined
     * @param value   the value, which does not necessarily have to exist in the array, from which to find the previous value
     * @return  the greatest value strictly less than the given value
     */
    Optional<ArrayValue<T>> previous(T value);

    /**
     * Returns the smallest value strictly greater than the given value
     * This operation only works if the array is sorted, otherwise result is undefined
     * @param value   the value, which does not necessarily have to exist in the array, from which to find the next value
     * @return      the least value strictly greater than the given value
     */
    Optional<ArrayValue<T>> next(T value);

    /**
     * Returns the stats interface to this array
     * @return  the stats interface to this array
     */
    Stats<Number> stats();

    /**
     * Fills this array with the value provided
     * @param value     the fill value
     * @return          this array reference
     */
    Array<T> fill(T value);

    /**
     * Fills a subset of the elements of this array with the value provided
     * @param value     the fill value
     * @param start     the start index in array, inclusive
     * @param end       the end index in array, exclusive
     * @return          this array reference
     */
    Array<T> fill(T value, int start, int end);

    /**
     * Shuffles the contents of this array
     * @param count     the number of rounds to shuffle
     * @return          this array reference
     */
    Array<T> shuffle(int count);

    /**
     * Swaps the values in this array at the two locations specified
     * @param i the first index of value to swap
     * @param j the second index of value to swap
     * @return  this array instance
     */
    Array<T> swap(int i, int j);

    /**
     * Sorts all elements in this array in either ascending or descending order
     * @param ascending     true for ascending, false for descending order
     * @return              the sorted Array reference
     */
    Array<T> sort(boolean ascending);

    /**
     * Sorts a subset of elements in this array based on the range specified in either ascending or descending order
     * @param start         the start index for range, inclusive
     * @param end           the end index for range, exclusive
     * @param ascending     true for ascending, false for descending order
     * @return              the sorted Array reference
     */
    Array<T> sort(int start, int end, boolean ascending);

    /**
     * Sorts a subset of elements in this array as per the range, using the comparator specified
     * @param start         the start index for range, inclusive
     * @param end           the end index for range, exclusive
     * @param comparator    the custom comparator to sort array
     * @return              the sorted Array reference
     */
    Array<T> sort(int start, int end, Comparator<ArrayValue<T>> comparator);

    /**
     * Returns a newly created array including values that pass the predicate
     * @param predicate the predicate to filter the array
     * @return          the filtered array
     */
    Array<T> filter(Predicate<ArrayValue<T>> predicate);

    /**
     * Expand this array to the new length specified
     * @param newLength the new expanded length for array
     * @return          this array instance
     */
    Array<T> expand(int newLength);

    /**
     * Returns -1, 0, 1 if the value at i is less than, equal to, or greater than the value at j
     * @param i    the index to the first value
     * @param j    the index to the second value
     * @return  -1, 0, 1 if the value at i is less than, equal to, or greater than the value at j
     */
    int compare(int i, int j);

    /**
     * Performs a binary search to find the index in this array which contains the value specified
     * This operation only works if the array is sorted, otherwise result is undefined
     * @param value     the value to search for, cannot be null
     * @return          index of the search key, if it is contained in the array; otherwise, (-(insertion point) - 1)
     */
    int binarySearch(T value);

    /**
     * Performs a binary search to find the index in this array which contains the value specified
     * This operation only works if the array is sorted, otherwise result is undefined
     * @param start     the start index for search scope
     * @param end       the end index for search scope
     * @param value     the value to search for, cannot be null
     * @return          index of the search key, if it is contained in the array; otherwise, (-(insertion point) - 1)
     */
    int binarySearch(int start, int end, T value);

    /**
     * Returns the number of entries that match the predicate specified
     * @param predicate the predicate to count matches
     * @return          the number of matches in array
     */
    int count(Predicate<ArrayValue<T>> predicate);

    /**
     * Updates certain elements of this array from the update provided, expanding this array if necessary
     * Note that the fromIndexes and toIndexes lengths must be equal
     * @param from          the array containing update values
     * @param fromIndexes   the indexes of update values in the from array
     * @param toIndexes     the indexes in this array to apply the updates to
     * @return              this array reference
     */
    Array<T> update(Array<T> from, int[] fromIndexes, int[] toIndexes);

    /**
     * Updates a contiguous block of data in this array from data in the array provided
     * @param toIndex       the start index in this array to write to
     * @param from          the array to consume updates from
     * @param fromIndex     to start index in the argument array to read from
     * @param length        the number of elements to update
     * @return              this array reference
     */
    Array<T> update(int toIndex, Array<T> from, int fromIndex, int length);

    /**
     * Maps all elements in this array using the function provided
     * @param func  the value generating function
     * @return      a newly created array with mapped values
     */
    <R> Array<R> map(Function<ArrayValue<T>,R> func);

    /**
     * Maps all elements in this array using the function provided
     * @param func  the boolean generating function
     * @return      a newly created array with mapped values
     */
    Array<Boolean> mapToBooleans(ToBooleanFunction<ArrayValue<T>> func);

    /**
     * Maps all elements in this array using the function provided
     * @param func  the int generating function
     * @return      a newly created array with mapped values
     */
    Array<Integer> mapToInts(ToIntFunction<ArrayValue<T>> func);

    /**
     * Maps all elements in this array using the function provided
     * @param func  the long generating function
     * @return      a newly created array with mapped values
     */
    Array<Long> mapToLongs(ToLongFunction<ArrayValue<T>> func);

    /**
     * Maps all elements in this array to double values using the function provided
     * @param func  the double generating function
     * @return      a newly created array with mapped values
     */
    Array<Double> mapToDoubles(ToDoubleFunction<ArrayValue<T>> func);

    /**
     * Applies the boolean generating function to all elements of this array
     * @param func  the boolean generating function
     * @return      this array reference
     */
    Array<T> applyBooleans(ToBooleanFunction<ArrayValue<T>> func);

    /**
     * Applies the int generating function to all elements of this array
     * @param func  the int generating function
     * @return      this array reference
     */
    Array<T> applyInts(ToIntFunction<ArrayValue<T>> func);

    /**
     * Applies the long generating function to all elements of this array
     * @param func  the long generating function
     * @return      this array reference
     */
    Array<T> applyLongs(ToLongFunction<ArrayValue<T>> func);

    /**
     * Applies the double generating function to all elements of this array
     * @param func  the double generating function
     * @return      this array reference
     */
    Array<T> applyDoubles(ToDoubleFunction<ArrayValue<T>> func);

    /**
     * Applies the value generating function to all elements of this array
     * @param func  the value generating function
     * @return      this array reference
     */
    Array<T> applyValues(Function<ArrayValue<T>,T> func);

    /**
     * Iterates over all elements in this array calling the consumer with each value
     * @param consumer  the array consumer for booleans
     * @return          this array reference
     */
    Array<T> forEachBoolean(BooleanConsumer consumer);

    /**
     * Iterates over all elements in this array calling the consumer with each value
     * @param consumer  the array consumer for ints
     * @return          this array reference
     */
    Array<T> forEachInt(IntConsumer consumer);

    /**
     * Iterates over all elements in this array calling the consumer with each value
     * @param consumer  the array consumer for longs
     * @return          this array reference
     */
    Array<T> forEachLong(LongConsumer consumer);

    /**
     * Iterates over all elements in this array calling the consumer with each value
     * @param consumer  the array consumer for doubles
     * @return          this array reference
     */
    Array<T> forEachDouble(DoubleConsumer consumer);

    /**
     * Iterates over all elements in this array calling the consumer with each value
     * @param consumer  the array consumer which takes this array value
     * @return          this array reference
     */
    Array<T> forEachValue(Consumer<ArrayValue<T>> consumer);

    /**
     * Returns true if the value at the specified index is null
     * @param index the index in this array
     * @return      true if the value is null
     */
    boolean isNull(int index);

    /**
     * Returns true if the value at the specified index is equal to the value specified
     * @param index the index in this array
     * @param value the value to check for equality to
     * @return      true if the element at index equals the argument provided
     */
    boolean isEqualTo(int index, T value);

    /**
     * Returns the value located at the index specified
     * @param index the index in this array
     * @return      the value for index
     */
    boolean getBoolean(int index);

    /**
     * Returns the value located at the index specified
     * @param index the index in this array
     * @return      the value for index
     */
    int getInt(int index);

    /**
     * Returns the value located at the index specified
     * @param index the index in this array
     * @return      the value for index
     */
    long getLong(int index);

    /**
     * Returns the value located at the index specified
     * @param index the index in this array
     * @return      the value for index
     */
    double getDouble(int index);

    /**
     * Returns the value located at the index specified
     * @param index the index in this array
     * @return      the value for index
     */
    T getValue(int index);

    /**
     * Sets the value located at the index specified
     * @param index the index in this array
     * @param value the value to set
     * @return  the previous value for index
     */
    boolean setBoolean(int index, boolean value);

    /**
     * Sets the value located at the index specified
     * @param index the index in this array
     * @param value the value to set
     * @return  the previous value for index
     */
    int setInt(int index, int value);

    /**
     * Sets the value located at the index specified
     * @param index the index in this array
     * @param value the value to set
     * @return  the previous value for index
     */
    long setLong(int index, long value);

    /**
     * Sets the value located at the index specified
     * @param index the index in this array
     * @param value the value to set
     * @return  the previous value for index
     */
    double setDouble(int index, double value);

    /**
     * Sets the value located at the index specified
     * @param index the index in this array
     * @param value the value to set
     * @return  the previous value for index
     */
    T setValue(int index, T value);

    /**
     * Reads content for this array from the input stream
     * @param is        the input stream to read from
     * @param count     the number of records to read
     * @throws IOException  if there is an I/O exception
     */
    void read(ObjectInputStream is, int count) throws IOException;

    /**
     * Writes content for this array to the output stream
     * @param os        the output stream to write to
     * @param indexes   the indexes of records to write
     * @throws IOException  if there is an I/O exception
     */
    void write(ObjectOutputStream os, int[] indexes) throws IOException;


    /**
     * Returns an empty array reference
     * @return      an empty array
     */
    static <V> Array<V> empty(Class<V> type) {
        return Array.of(type, 0, 1F);
    }

    /**
     * Returns a newly created memory mapped array of the type specified
     * A temporary file will be created for this array which will be deleted on JVM exit
     * @param type          the element type for array
     * @param length        the initial length of the array
     * @param <V>           the type
     * @return              the newly created memory mapped array
     */
    static <V> Array<V> map(Class<V> type, int length) {
        return ArrayFactory.mapped().apply(type, length, ArrayType.defaultValue(type));
    }

    /**
     * Returns a newly created memory mapped array of the type specified
     * A temporary file will be created for this array which will be deleted on JVM exit
     * @param type          the element type for array
     * @param length        the initial length of the array
     * @param defaultValue  the default value for the array
     * @param <V>           the type
     * @return              the newly created memory mapped array
     */
    static <V> Array<V> map(Class<V> type, int length, V defaultValue) {
        return ArrayFactory.mapped().apply(type, length, defaultValue);
    }

    /**
     * Returns a newly created memory mapped array of the type specified using the file provided
     * @param type          the element type for array
     * @param length        the initial length of the array
     * @param defaultValue  the default value for the array
     * @param path          the file path which may or may not exist (directories will be created)
     * @param <V>           the type for array
     * @return              the newly created memory mapped array
     */
    static <V> Array<V> map(Class<V> type, int length, V defaultValue, String path) {
        return ArrayFactory.mapped().apply(type, length, defaultValue, path);
    }

    /**
     * Returns a newly created dense Array that wraps the object array specified
     * @param values    the values to wrap
     * @return          the newly created Array
     */
    @SuppressWarnings("unchecked")
    static <V> Array<V> of(V... values) {
        return ArrayFactory.create(values);
    }

    /**
     * Returns a newly created dense Array that wraps the boolean array specified
     * @param values    the values to wrap
     * @return          the newly created Array
     */
    static Array<Boolean> of(boolean[] values) {
        return ArrayFactory.create(values);
    }

    /**
     * Returns a newly created dense Array that wraps the integer array specified
     * @param values    the values to wrap
     * @return          the newly created Array
     */
    static Array<Integer> of(int[] values) {
        return ArrayFactory.create(values);
    }

    /**
     * Returns a newly created dense Array that wraps the long array specified
     * @param values    the values to wrap
     * @return          the newly created Array
     */
    static Array<Long> of(long[] values) {
        return ArrayFactory.create(values);
    }

    /**
     * Returns a newly created dense Array that wraps the double array specified
     * @param values    the values to wrap
     * @return          the newly created Array
     */
    static Array<Double> of(double[] values) {
        return ArrayFactory.create(values);
    }

    /**
     * Returns a newly created dense Array based on the arguments specified
     * @param type          the data type for Array
     * @param length        the initial length for array
     * @return              the newly created array
     */
    static <V> Array<V> of(Class<V> type, int length) {
        return Array.of(type, length, 1F);
    }

    /**
     * Returns a newly created dense Array based on the arguments specified
     * @param type          the data type for Array
     * @param length        the initial length for array
     * @param defaultValue  the default value for the array
     * @return              the newly created array
     */
    static <V> Array<V> of(Class<V> type, int length, V defaultValue) {
        return Array.of(type, length, defaultValue, 1F);
    }

    /**
     * Returns a newly created dense Array based on the arguments specified
     * @param type          the data type for Array
     * @param length        the initial length for array
     * @param defaultValue  the default value for the array
     * @return              the newly created array
     */
    static <V> Array<V> of(Class<V> type, int length, V defaultValue, ArrayStyle style) {
        switch (style) {
            case DENSE:     return ArrayFactory.dense().apply(type, length, defaultValue);
            case SPARSE:    return ArrayFactory.sparse().apply(type, length, defaultValue);
            case MAPPED:    return ArrayFactory.mapped().apply(type, length, defaultValue);
            default:        throw new IllegalArgumentException("Unsupported style specified: " + style);
        }
    }

    /**
     * Returns a newly created Array based on the arguments specified
     * @param type          the data type for Array
     * @param length        the initial length for array
     * @param loadFactor    the load factor between 0..1 (1 for dense array, < 1 for sparse array)
     * @return              the newly created array
     */
    static <V> Array<V> of(Class<V> type, int length, float loadFactor) {
        return Array.of(type, length, ArrayType.defaultValue(type), loadFactor);
    }

    /**
     * Returns a newly created Array based on the arguments specified
     * @param type          the data type for Array
     * @param length        the initial length for array
     * @param defaultValue  the default value for the array
     * @param loadFactor    the load factor between 0..1 (1 for dense array, < 1 for sparse array)
     * @return              the newly created array
     */
    static <V> Array<V> of(Class<V> type, int length, V defaultValue, float loadFactor) {
        if (loadFactor < 1f) {
            return ArrayFactory.sparse().apply(type, length, defaultValue);
        } else {
            return ArrayFactory.dense().apply(type, length, defaultValue);
        }
    }

    /**
     * Returns a newly created Array to hold data of the type specified
     * @param type      the array type definition
     * @param values    the values for array
     * @return          the newly created array
     */
    @SafeVarargs
    static <V> Array<V> of(Class<V> type, V... values) {
        return Array.of(type, values.length).applyValues(v -> values[v.index()]);
    }

    /**
     * Returns a newly created Array of objects of length specified
     * @param length    the initial length for array
     * @return          the newly created array
     */
    @SuppressWarnings("unchecked")
    static <V> Array<V> ofObjects(int length) {
        return of((Class<V>)Object.class, length);
    }

    /**
     * Returns a newly created Array of objects of length specified
     * @param length        the initial length for array
     * @param loadFactor    the load factor between 0..1 (1 for dense array, < 1 for sparse array)
     * @return              the newly created array
     */
    @SuppressWarnings("unchecked")
    static <V> Array<V> ofObjects(int length, float loadFactor) {
        return of((Class<V>)Object.class, length, loadFactor);
    }

    /**
     * Returns a newly created Array of objects of length specified
     * @param length        the initial length for array
     * @param defaultValue  the default value for the array
     * @param loadFactor    the load factor between 0..1 (1 for dense array, < 1 for sparse array)
     * @return              the newly created array
     */
    @SuppressWarnings("unchecked")
    static <V> Array<V> ofObjects(int length, V defaultValue, float loadFactor) {
        return of((Class<V>)Object.class, length, defaultValue, loadFactor);
    }


    /**
     * Returns a newly created dense Array with the contents of the list
     * @param values    the values to expose as an array
     * @return          the newly created Array
     */
    static <V> Array<V> ofIterable(Iterable<V> values) {
        return ArrayBuilder.<V>of(1000).addAll(values).toArray();
    }

    /**
     * Returns a newly created array by concatenating the input arrays provided
     * @param type      the element type for new array
     * @param arrays    the input arrays to concatenate in order
     * @param <V>       the array element type
     * @return          the resulting array
     */
    static <V> Array<V> concat(Class<V> type, Iterable<Array<V>> arrays) {
        return ArrayFactory.concat(type, arrays);
    }

    /**
     * Convenience method to create an array of length 1 containing the specified value
     * @param value     the singleton value for the array
     * @param <V>       the element type
     * @return          the newly created array
     */
    @SuppressWarnings("unchecked")
    static <V> Array<V> singleton(V value) {
        return Array.of((Class<V>)value.getClass(), 1).applyValues(v -> value);
    }


    /**
     * Returns a sparse Array representation of the source array, if it is not already sparse
     * @param source    the source array
     * @param <T>       the array element type
     * @return          the source array if it is already sparse, or a newly created sparse array
     */
    static <T> Array<T> sparse(Array<T> source) {
        if (source.style().isSparse()) {
            return source;
        } else {
            final T defaultValue = source.defaultValue();
            final int count = source.count(v -> v.isEqualTo(defaultValue));
            final float loadFactor = (float)(Math.abs(count - source.length())) / (float)source.length();
            final float loadFactorAdjusted = Math.min(0.99F, loadFactor);
            final Array<T> result = Array.of(source.type(), source.length(), defaultValue, loadFactorAdjusted);
            switch (source.typeCode()) {
                case BOOLEAN:           result.applyBooleans(v -> source.getBoolean(v.index()));    break;
                case INTEGER:           result.applyInts(v -> source.getInt(v.index()));            break;
                case LONG:              result.applyLongs(v -> source.getLong(v.index()));          break;
                case DOUBLE:            result.applyDoubles(v -> source.getDouble(v.index()));      break;
                case DATE:              result.applyLongs(v -> source.getLong(v.index()));          break;
                case LOCAL_DATE:        result.applyLongs(v -> source.getLong(v.index()));          break;
                case LOCAL_TIME:        result.applyLongs(v -> source.getLong(v.index()));          break;
                case LOCAL_DATETIME:    result.applyLongs(v -> source.getLong(v.index()));          break;
                case INSTANT:           result.applyLongs(v -> source.getLong(v.index()));          break;
                default:                result.applyValues(v -> source.getValue(v.index()));        break;
            }
            return result;
        }
    }


    /**
     * Returns a dense Array representation of the source array, if it is not already dense
     * @param source    the source array
     * @param <T>       the array element type
     * @return          the source array if it is already dense, or a newly created dense array
     */
    static <T> Array<T> dense(Array<T> source) {
        if (source.style().isDense()) {
            return source;
        } else {
            final T defaultValue = source.defaultValue();
            final Array<T> result = Array.of(source.type(), source.length(), defaultValue);
            switch (source.typeCode()) {
                case BOOLEAN:           result.applyBooleans(v -> source.getBoolean(v.index()));    break;
                case INTEGER:           result.applyInts(v -> source.getInt(v.index()));            break;
                case LONG:              result.applyLongs(v -> source.getLong(v.index()));          break;
                case DOUBLE:            result.applyDoubles(v -> source.getDouble(v.index()));      break;
                case DATE:              result.applyLongs(v -> source.getLong(v.index()));          break;
                case LOCAL_DATE:        result.applyLongs(v -> source.getLong(v.index()));          break;
                case LOCAL_TIME:        result.applyLongs(v -> source.getLong(v.index()));          break;
                case LOCAL_DATETIME:    result.applyLongs(v -> source.getLong(v.index()));          break;
                case INSTANT:           result.applyLongs(v -> source.getLong(v.index()));          break;
                default:                result.applyValues(v -> source.getValue(v.index()));        break;
            }
            return result;
        }
    }

}
