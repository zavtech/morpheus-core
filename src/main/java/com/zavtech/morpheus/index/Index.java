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
package com.zavtech.morpheus.index;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBuilder;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.util.IntComparator;

/**
 * An interface to a data structure that maintains an ordered set of keys and a canonical index for each key.
 *
 * <p>This interface exposes an ordered set of keys that are each mapped to an immutable index value, which is
 * generally used to represent the in-memory position of the data item associated with the key. The order
 * of the keys in the <code>Index</code> can change, but their associated index value remains constant, hence
 * why they are referred to as a canonical index in this documentation. The canonical index differs from the
 * ordinal value which can change as keys are re-ordered in this <code>Index</code>, and the ordinal expresses
 * ordering as the name implies.</p>
 *
 * @param <K>   the index element type
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface Index<K> extends Iterable<K>, Cloneable, Serializable {

    /**
     * Returns the size of this index
     * @return  the index size
     */
    int size();

    /**
     * Returns the current capacity of this index
     * @return  the current capacity of this index
     */
    int capacity();

    /**
     * Adds a key to this index if it does not already exist
     * @param key   the key reference
     * @return      true if ket was added, false if already existed
     */
    boolean add(K key);

    /**
     * Adds multiple keys to this index
     * @param keys  the keys to add
     * @param ignoreDuplicates  if true, ignore duplicates, otherwise raise an exception
     * @return      the number of keys added to this index
     * @throws IndexException   if ignoreDuplicates is false and attempt to add keys that already exist
     */
    int addAll(Iterable<K> keys, boolean ignoreDuplicates);

    /**
     * Returns the key type for this index
     * @return  the key type for this index
     */
    Class<K> type();

    /**
     * Returns true if this index is empty
     * @return  true if index is empty
     */
    boolean isEmpty();

    /**
     * Returns true if this is a filter on another index
     * @return  true if is a filter on another index
     */
    boolean isFilter();

    /**
     * Returns true if this index is read-only and therefore keys cannot be added
     * @return      true if this index is read-only, so keys cannot be added
     */
    boolean isReadOnly();

    /**
     * Returns a deep copy of this index
     * @return  a deep copy of this index
     */
    Index<K> copy();

    /**
     * Returns a read-only shallow copy of this index
     * @return  a read only shallow copy
     */
    Index<K> readOnly();

    /**
     * Retruns the keys in this index as a stream
     * @return  the keys as a stream
     */
    Stream<K> keys();

    /**
     * Returns an unmodifiable list of the keys in this inde
     * @return  an unmodifiable list of keys
     */
    List<K> toList();

    /**
     * Retruns the keys in this index as a array
     * @return  the keys as a array
     */
    Array<K> toArray();

    /**
     * Returns the keys in this index for the range specified
     * @param from  the from ordinal (inclusive)
     * @param to    the to ordinal (exclusive)
     * @return  the keys for this index in the range specified
     */
    Array<K> toArray(int from, int to);

    /**
     * Returns the canonical indexes for this index ordered according to key order
     * @return      the stream of canonical indexes
     */
    IntStream indexes();

    /**
     * Returns the canonical indexes for the keys specified
     * @param keys  the keys to select indexes for
     * @return      the stream of canonical indexes for the keys specified
     * @throws  IndexException  if no match for one of the keys
     */
    IntStream indexes(Iterable<K> keys);

    /**
     * Returns the view ordinals for this index for the keys specified
     * @param keys  the keys to select view ordinals
     * @return      the stream of view ordinals for the keys specified
     * @throws  IndexException  if no match for one of the keys
     */
    IntStream ordinals(Iterable<K> keys);

    /**
     * Returns a reference to the first entry in this index
     * @return      the first entry in the index
     */
    Optional<K> first();

    /**
     * Returns a reference to the last entry in the index
     * @return      the last entry in the index
     */
    Optional<K> last();

    /**
     * Returns the largest key strictly less than the given key
     * This operation only works if the index is sorted, otherwise result is undefined
     * @param key   the key from which to find the next lower key
     * @return      the largest key strictly less than the given key
     */
    Optional<K> previousKey(K key);

    /**
     * Returns the smallest key strictly greater than the given key
     * This operation only works if the array is sorted, otherwise result is undefined
     * @param key   the key from which to find the next highest key
     * @return      the smallest key strictly greater than the given key
     */
    Optional<K> nextKey(K key);

    /**
     * Resets the order of this index to insertion order
     * @return  this index
     */
    Index<K> resetOrder();

    /**
     * Returns the key for the ordinal specified
     * @param ordinal   the ordinal for requested key
     * @return          the key for ordinal
     */
    K getKey(int ordinal);

    /**
     * Returns the ordinal for the key specified
     * @param key   the key for which to find the respective ordinal
     * @return      the ordinal for key
     * @throws IndexException   if there is no match for key
     */
    int getOrdinalForKey(K key);

    /**
     * Returns the ordinal for the canonical index specified
     * @param index the index to find the respective ordinal
     * @return      the ordinal for canonical index
     */
    int getOrdinalForIndex(int index);

    /**
     * Returns the canonical index for the key specified
     * @param key   the key for which to find the canonical index
     * @return      the canonical index for key
     * @throws IndexException   if there is no match for key
     */
    int getIndexForKey(K key);

    /**
     * Returns the canonical index for the ordinal specified
     * @param ordinal   the ordinal of the key for which to find the canonical index
     * @return          the canonical index for the ordinal
     */
    int getIndexForOrdinal(int ordinal);

    /**
     * Returns true if this index contains the specified key
     * @param key   the key to check exists
     * @return      true if key exists
     */
    boolean contains(K key);

    /**
     * Returns true if this index contains all keys in the specified stream
     * @param keys  the stream of keys to check for inclusion
     * @return      true if this index contains all keys in the stream
     */
    boolean containsAll(Iterable<K> keys);

    /**
     * Maps this Index into a new Index based on the mapper provided
     * @param mapper    the index mapper that takes the key, ordinal and index
     * @param <V>       the new Index type
     * @return          the newly created Index
     */
    <V> Index<V> map(IndexMapper<K,V> mapper);

    /**
     * Returns an array of keys that intersect with the stream
     * @param keys  the stream of keys to find the intersection
     * @return      the array of intersecting keys
     */
    Array<K> intersect(Iterable<K> keys);

    /**
     * Replaces the existing key with the replacement key in this index
     * @param existing      the existing key
     * @param replacement   the replacement key
     * @return              the index allocated to replacement key
     * @throws IndexException   if the existing key does not exist, or the replacement key already exists
     */
    int replace(K existing, K replacement);

    /**
     * Iterates over all entries in this index and calls the consumer
     * @param consumer  the consumer to receive callbacks for each entry in the index
     */
    void forEachEntry(IndexConsumer<K> consumer);

    /**
     * Sorts the keys in this index in ascending or descending order
     * @param parallel      true for parallel sort, false for sequential
     * @param ascending     true for ascending, false for descending
     * @return              this index
     */
    Index<K> sort(boolean parallel, boolean ascending);

    /**
     * Sorts the keys in this index according to the comparator specified
     * @param parallel      true for parallel sort, false for sequential
     * @param comparator    the user provided compaator
     * @return              this index
     */
    Index<K> sort(boolean parallel, IntComparator comparator);

    /**
     * Returns a filter over this index including only the keys specified
     * @param keys  the iterable set of keys to include in the filter
     * @return      the filtered index
     * @throws IndexException   if one of the keys in the array does not exist in the index
     */
    Index<K> filter(Iterable<K> keys);

    /**
     * Returns a filter over this index based on the provided predicate
     * @param predicate the predicate to filter keys
     * @return          the filtered index
     */
    Index<K> filter(Predicate<K> predicate);

    /**
     * Returns a newly created index based on the array provided
     * @param keys      the keys for index
     * @param <K>       the element type
     * @return          the newly created Index
     */
    static <K> Index<K> of(Iterable<K> keys) {
        return IndexFactory.getInstance().create(keys);
    }

    /**
     * Returns a newly created index based on the array provided
     * @param keys      the keys for index
     * @param <K>       the element type
     * @return          the newly created Index
     */
    @SafeVarargs
    static <K> Index<K> of(Class<K> type, K... keys) {
        return IndexFactory.getInstance().create(Array.of(type, keys));
    }

    /**
     * Returns a newly created index based on the type and initial size provided
     * @param type          the type for index
     * @param initialSize   the initial size of Index
     * @param <K>           the element type
     * @return              the newly created Index
     */
    static <K> Index<K> of(Class<K> type, int initialSize) {
        return IndexFactory.getInstance().create(type, initialSize);
    }

    /**
     * Returns a newly created index based on the type and initial size provided
     * @param type          the type for index
     * @param keys          the initial keys for Index
     * @param <K>           the element type
     * @return              the newly created Index
     */
    static <K> Index<K> of(Class<K> type, Iterable<K> keys) {
        if (keys instanceof Array) {
            final int capacity = ((Array)keys).length();
            return IndexFactory.getInstance().create(type, capacity);
        } else if (keys instanceof Range) {
            final int capacity = (int)((Range)keys).estimateSize();
            return IndexFactory.getInstance().create(type, capacity);
        } else if (keys instanceof Collection) {
            final int capacity = ((Collection)keys).size();
            return IndexFactory.getInstance().create(type, capacity);
        } else {
            final Array<K> values = ArrayBuilder.of(10000, type).addAll(keys).toArray();
            return Index.of(values);
        }
    }

    /**
     * Retruns an empty Index that is immutable
     * @param <K>   the index element type
     * @return      the empty Index
     */
    @SuppressWarnings("unchecked")
    static <K> Index<K> empty() {
        return Index.of((Class<K>)Object.class, 0);
    }

    /**
     * Returns an index built from a single key
     * @param key   the single key to wrap in index
     * @param <K>   the key type
     * @return      the newly created index
     */
    static <K> Index<K> singleton(K key) {
        return Index.of(Array.singleton(key));
    }

    /**
     * Returns a newly created index to hold values provided
     * @param values    the values to build index from
     * @return          the newly created Index
     */
    @SafeVarargs
    static <V> Index<V> of(V... values) {
        return IndexFactory.getInstance().create(Array.of(values));
    }

}
