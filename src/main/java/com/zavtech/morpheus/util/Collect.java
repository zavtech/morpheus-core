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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A convenience factory class for building collections of various kinds
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class Collect {


    /**
     * Private constructor
     */
    private Collect() {
        super();
    }

    /**
     * Returns a new array List of the values specified
     * @param values    the values to create a new Set from
     * @param <T>       the element type
     * @return          the newly created set
     */
    @SafeVarargs
    public static <T> List<T> asList(T... values) {
        return Collect.asList(false, values);
    }


    /**
     * Returns a new array List of the values specified
     * @param values    the values to create a new Set from
     * @param <T>       the element type
     * @return          the newly created set
     */
    public static <T> List<T> asList(Iterable<T> values) {
        return Collect.asList(false, values);
    }


    /**
     * Returns a new Stream of the values from the Iterator
     * @param values    the values to create a new Set from
     * @param <T>       the element type
     * @return          the newly created set
     */
    public static <T> Stream<T> asStream(Iterator<T> values) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(values, Spliterator.ORDERED), false);
    }


    /**
     * Returns a new Stream of the values from the Iterable
     * @param values    the values to create a new Set from
     * @param <T>       the element type
     * @return          the newly created set
     */
    public static <T> Stream<T> asStream(Iterable<T> values) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(values.iterator(), Spliterator.ORDERED), false);
    }


    /**
     * Returns a new linked List of the values specified
     * @param values    the values to create a new Set from
     * @param <T>       the element type
     * @return          the newly created set
     */
    @SafeVarargs
    public static <T> List<T> asList(boolean linked, T... values) {
        if (linked) {
            final List<T> result = new LinkedList<>();
            for (T value : values) result.add(value);
            return result;
        } else {
            final List<T> result = new ArrayList<>(values.length);
            for (T value : values) result.add(value);
            return result;
        }
    }


    /**
     * Returns a new linked List of the values specified
     * @param values    the values to create a new Set from
     * @param <T>       the element type
     * @return          the newly created set
     */
    public static <T> List<T> asList(boolean linked, Iterable<T> values) {
        if (linked) {
            final List<T> result = new LinkedList<>();
            for (T value : values) result.add(value);
            return result;
        } else {
            final List<T> result = new ArrayList<>();
            for (T value : values) result.add(value);
            return result;
        }
    }


    /**
     * Returns a new Set of the values specified
     * @param values    the values to create a new Set from
     * @param <T>       the element type
     * @return          the newly created set
     */
    @SafeVarargs
    public static <T> Set<T> asSet(T... values) {
        final Set<T> result = new HashSet<>(values.length);
        for (T value : values) result.add(value);
        return result;
    }

    /**
     * Returns a new Set of the values specified
     * @param values    the values to create a new Set from
     * @param <T>       the element type
     * @return          the newly created set
     */
    @SafeVarargs
    public static <T> SortedSet<T> asSortedSet(T... values) {
        final SortedSet<T> result = new TreeSet<>();
        for (T value : values) result.add(value);
        return result;
    }

    /**
     * Returns a new created Map initialized with whatever the consumer does
     * @param mapper            the consumer that sets up mappings
     * @param <K>               the key type
     * @param <V>               the value type
     * @return                  the newly created apply
     */
    public static <K,V> Map<K,V> asMap(Consumer<Map<K,V>> mapper) {
        final Map<K,V> map = new HashMap<>();
        mapper.accept(map);
        return map;
    }

    /**
     * Returns a new created Map initialized with whatever the consumer does
     * @param initialCapacity   the initial capacity for apply
     * @param mapper            the consumer that sets up mappings
     * @param <K>               the key type
     * @param <V>               the value type
     * @return                  the newly created apply
     */
    public static <K,V> Map<K,V> asMap(int initialCapacity, Consumer<Map<K,V>> mapper) {
        final Map<K,V> map = new HashMap<>(initialCapacity);
        mapper.accept(map);
        return map;
    }

    /**
     * Returns a new created Map initialized with whatever the consumer does
     * @param mapper            the consumer that sets up mappings
     * @param <K>               the key type
     * @param <V>               the value type
     * @return                  the newly created apply
     */
    public static <K,V> SortedMap<K,V> asSortedMap(Consumer<Map<K,V>> mapper) {
        final SortedMap<K,V> map = new TreeMap<>();
        mapper.accept(map);
        return map;
    }

    /**
     * Returns a apply that reverses the input apply
     * @param map   the apply reference to reverse
     * @param <K>   the type for key
     * @param <V>   the type for value
     * @return      the reverse mapped
     */
    public static <K,V> Map<V,K> reverse(Map<K,V> map) {
        if (map instanceof SortedMap) {
            final Map<V,K> result = new TreeMap<>();
            map.forEach((key, value) -> result.put(value, key));
            return result;
        } else if (map instanceof LinkedHashMap) {
            final Map<V,K> result = new LinkedHashMap<>(map.size());
            map.forEach((key, value) -> result.put(value, key));
            return result;
        } else {
            final Map<V,K> result = new HashMap<>(map.size());
            map.forEach((key, value) -> result.put(value, key));
            return result;
        }
    }
}
