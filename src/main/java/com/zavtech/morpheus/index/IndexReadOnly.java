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

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.util.IntComparator;

/**
 * A read-only decorator for an Index<K> that does not allow keys to be added.
 *
 * @param <K>   the index element type
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
class IndexReadOnly<K> implements Index<K> {

    private static final long serialVersionUID = 1L;

    private Index<K> underlying;

    /**
     * Constructor
     * @param underlying    the underlying index
     */
    IndexReadOnly(Index<K> underlying) {
        this.underlying = underlying;
    }

    @Override
    public final int size() {
        return underlying.size();
    }

    @Override
    public final int capacity() {
        return underlying.capacity();
    }

    @Override
    public final boolean add(K key) {
        throw new IndexException("The Index in question is ready only");
    }

    @Override
    public final int addAll(Iterable<K> keys, boolean ignoreDuplicates) {
        throw new IndexException("The Index in question is ready only");
    }

    @Override
    public final Class<K> type() {
        return underlying.type();
    }

    @Override
    public final boolean isEmpty() {
        return underlying.isEmpty();
    }

    @Override
    public final boolean isFilter() {
        return underlying.isFilter();
    }

    @Override
    public final boolean isReadOnly() {
        return true;
    }

    @Override
    public final Index<K> copy() {
        return underlying.copy();
    }

    @Override
    public final Index<K> readOnly() {
        return this;
    }

    @Override
    public final Stream<K> keys() {
        return underlying.keys();
    }

    @Override
    public final List<K> toList() {
        return underlying.toList();
    }

    @Override
    public final Array<K> toArray() {
        return underlying.toArray();
    }

    @Override
    public final Array<K> toArray(int from, int to) {
        return underlying.toArray(from, to);
    }

    @Override
    public final IntStream indexes() {
        return underlying.indexes();
    }

    @Override
    public final IntStream indexes(Iterable<K> keys) {
        return underlying.indexes(keys);
    }

    @Override
    public final IntStream ordinals(Iterable<K> keys) {
        return underlying.ordinals(keys);
    }

    @Override
    public final Optional<K> first() {
        return underlying.first();
    }

    @Override
    public final Optional<K> last() {
        return underlying.last();
    }

    @Override
    public final Optional<K> previousKey(K key) {
        return underlying.previousKey(key);
    }

    @Override
    public final Optional<K> nextKey(K key) {
        return underlying.nextKey(key);
    }

    @Override
    public final Index<K> resetOrder() {
        return underlying.resetOrder();
    }

    @Override
    public final K getKey(int ordinal) {
        return underlying.getKey(ordinal);
    }

    @Override
    public final int getOrdinalForKey(K key) {
        return underlying.getOrdinalForKey(key);
    }

    @Override
    public final int getOrdinalForIndex(int index) {
        return underlying.getOrdinalForIndex(index);
    }

    @Override
    public final int getIndexForKey(K key) {
        return underlying.getIndexForKey(key);
    }

    @Override
    public final int getIndexForOrdinal(int ordinal) {
        return underlying.getIndexForOrdinal(ordinal);
    }

    @Override
    public final boolean contains(K key) {
        return underlying.contains(key);
    }

    @Override
    public final boolean containsAll(Iterable<K> keys) {
        return underlying.containsAll(keys);
    }

    @Override
    public final Array<K> intersect(Iterable<K> keys) {
        return underlying.intersect(keys);
    }

    @Override
    public final int replace(K existing, K replacement) {
        throw new IndexException("The Index in question is ready only");
    }

    @Override
    public final void forEachEntry(IndexConsumer<K> consumer) {
        this.underlying.forEachEntry(consumer);
    }

    @Override
    public final Index<K> sort(boolean parallel, boolean ascending) {
        return underlying.sort(parallel, ascending);
    }

    @Override
    public final Index<K> sort(boolean parallel, IntComparator comparator) {
        return underlying.sort(parallel, comparator);
    }

    @Override
    public final Index<K> filter(Iterable<K> keys) {
        return underlying.filter(keys);
    }

    @Override
    public final Index<K> filter(Predicate<K> predicate) {
        return underlying.filter(predicate);
    }

    @Override
    public final Iterator<K> iterator() {
        return underlying.iterator();
    }
}
