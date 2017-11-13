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
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBuilder;
import com.zavtech.morpheus.array.ArrayValue;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.util.IntComparator;
import com.zavtech.morpheus.util.SortAlgorithm;
import com.zavtech.morpheus.util.Swapper;

/**
 * A convenience base class for building Index implementations
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
abstract class IndexBase<K> implements Index<K> {

    private static final long serialVersionUID = 1L;

    private Array<K> keys;
    private Index<K> parent;
    private Array<Integer> indexes;
    private Array<Integer> ordinals;

    /**
     * Constructor
     *
     * @param keys the initial set of keys for this index
     */
    IndexBase(Iterable<K> keys) {
        this(keys, null);
    }

    /**
     * Constructor
     * @param iterable the iterable set of keys for this index
     * @param parent   the parent when creating a filter, null otherwise
     */
    IndexBase(Iterable<K> iterable, Index<K> parent) {
        this.keys = createArray(iterable);
        this.parent = parent;
        if (parent != null) {
            final float loadFactor = (float)keys.length() / (float)parent.size();
            this.indexes = Array.of(Integer.class, keys.length());
            this.ordinals = Array.of(Integer.class, parent.size(), loadFactor);
            for (int ordinal = 0; ordinal < keys.length(); ++ordinal) {
                final K key = keys.getValue(ordinal);
                final int index = parent.getIndexForKey(key);
                this.indexes.setInt(ordinal, index);
                this.ordinals.setInt(index, ordinal);
            }
        }
    }

    /**
     * Creates an array to hold the keys from the iterable
     * @param keys the iterable set of keys
     * @return an array to store keys for this index
     */
    @SuppressWarnings("unchecked")
    private Array<K> createArray(Iterable<K> keys) {
        if (keys instanceof Array) {
            return (Array<K>) keys;
        } else if (keys instanceof ArrayBuilder) {
            return ((ArrayBuilder<K>)keys).toArray();
        } else if (keys instanceof Index) {
            return ((Index<K>) keys).toArray();
        } else if (keys instanceof Range) {
            return ((Range<K>) keys).toArray();
        } else {
            final Iterator<K> iterator = keys.iterator();
            final Class<K> clazz = iterator.hasNext() ? (Class<K>) iterator.next().getClass() : (Class<K>) Object.class;
            return ArrayBuilder.of(1000, clazz).addAll(keys).toArray();
        }
    }

    /**
     * Returns the parent for this index
     * @return the optional parent
     */
    Index<K> parent() {
        return parent;
    }

    /**
     * Internal method to allow subclasses direct access to the internal key array
     * @return the key array for index
     */
    protected final Array<K> keyArray() {
        return keys;
    }

    @Override()
    public final int capacity() {
        return keys.length();
    }

    @Override()
    public final Class<K> type() {
        return keys.type();
    }

    @Override
    public final boolean isEmpty() {
        return size() == 0;
    }

    @Override()
    public final boolean isFilter() {
        return parent != null;
    }

    @Override()
    public final boolean isReadOnly() {
        return parent != null;
    }

    @Override
    public Index<K> readOnly() {
        return new IndexReadOnly<>(this);
    }

    @Override()
    public final Stream<K> keys() {
        return IntStream.range(0, size()).mapToObj(keys::getValue);
    }

    @Override
    public List<K> toList() {
        return keys.toList();
    }

    @Override()
    public final Array<K> toArray() {
        return keys.copy(0, size());
    }

    @Override()
    public final Array<K> toArray(int from, int to) {
        return keys.copy(from, to);
    }

    @Override()
    public final IntStream indexes() {
        return indexes != null ? indexes.stream().ints() : IntStream.range(0, size());
    }

    @Override()
    public final IntStream indexes(Iterable<K> keys) {
        return StreamSupport.stream(keys.spliterator(), false).mapToInt(this::getIndexForKey);
    }

    @Override()
    public final IntStream ordinals(Iterable<K> keys) {
        return StreamSupport.stream(keys.spliterator(), false).mapToInt(this::getOrdinalForKey);
    }

    @Override()
    public final Optional<K> first() {
        return keys.first(v -> true).map(ArrayValue::getValue);
    }

    @Override()
    public final Optional<K> last() {
        return size() == 0 ? Optional.empty() : Optional.ofNullable(keys.getValue(size() - 1));
    }

    @Override()
    public final Optional<K> previousKey(K key) {
        return keys.previous(key).map(ArrayValue::getValue);
    }

    @Override()
    public final Optional<K> nextKey(K key) {
        return keys.next(key).map(ArrayValue::getValue);
    }

    @Override
    public final K getKey(int ordinal) {
        return keys.getValue(ordinal);
    }

    @Override
    public final int getOrdinalForKey(K key) {
        final int index = getIndexForKey(key);
        return getOrdinalForIndex(index);
    }

    @Override
    public final int getOrdinalForIndex(int index) {
        if (ordinals == null) {
            return index;
        } else {
            return ordinals.getInt(index);
        }
    }

    @Override()
    public final int getIndexForOrdinal(int ordinal) {
        if (indexes == null) {
            return ordinal;
        } else {
            return indexes.getInt(ordinal);
        }
    }


    @Override
    public final boolean containsAll(Iterable<K> keys) {
        for (K key : keys) {
            if (!contains(key)) {
                return false;
            }
        }
        return true;
    }


    @Override
    public final <V> Index<V> map(IndexMapper<K,V> mapper) {
        if (parent != null) {
            throw new IndexException("Cannot map a filtered Index, call copy() first");
        } else {
            final Array<V> newKeys = Range.of(0, size()).map(i -> mapper.map(keys.getValue(i), i)).toArray();
            final Index<V> newIndex = Index.of(newKeys);
            if (newIndex instanceof IndexBase) {
                final IndexBase<V> base = (IndexBase<V>)newIndex;
                base.ordinals = this.ordinals;
                base.indexes = this.indexes;
            }
            return newIndex;
        }
    }


    @Override
    public Index<K> sort(boolean parallel, boolean ascending) {
        try {
            final int multiplier = ascending ? 1 : -1;
            this.indexes = indexes != null ? indexes : Range.of(0, size()).toArray();
            final IntComparator comparator = (i, j) -> multiplier * keys.compare(i, j);
            final Swapper swapper = (i, j) ->  { keys.swap(i, j); indexes.swap(i, j); };
            SortAlgorithm.getDefault(parallel).sort(0, size(), comparator, swapper);
            return this;
        } catch (Exception ex) {
            throw new RuntimeException("Failed to sort Index", ex);
        }
    }


    @Override
    public Index<K> sort(boolean parallel, IntComparator comparator) {
        try {
            if (comparator == null) {
                this.indexes = null;
                this.ordinals = null;
            } else {
                this.indexes = indexes != null ? indexes : Range.of(0, size()).toArray();
                final Swapper swapper = (i, j) ->  { keys.swap(i, j); indexes.swap(i, j); };
                SortAlgorithm.getDefault(parallel).sort(0, size(), comparator, swapper);
                this.ordinals = ordinals != null ? ordinals : Array.of(Integer.class, indexes.length());
                for (int i = 0; i < indexes.length(); ++i) {
                    final int index = indexes.getInt(i);
                    this.ordinals.setInt(index, i);
                }
            }
            return this;
        } catch (Exception ex) {
            throw new RuntimeException("Failed to sort Index", ex);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public Index<K> copy() {
        try {
            final IndexBase<K> clone = (IndexBase<K>)super.clone();
            clone.keys = keys.copy();
            clone.parent = parent;
            clone.indexes = indexes != null ? indexes.copy() : null;
            clone.ordinals = ordinals != null ? ordinals.copy() : null;
            return clone;
        } catch (Exception ex) {
            throw new IndexException("Failed to create deep copy of Index", ex);
        }
    }


    @Override()
    public Index<K> resetOrder() {
        if (!isFilter()) {
            this.ordinals = null;
            this.indexes = null;
        }
        return this;
    }


    @Override
    public final Array<K> intersect(Iterable<K> keys) {
        final int size = Math.max(100, (int) (size() * 0.2));
        final ArrayBuilder<K> builder = ArrayBuilder.of(size, this.keys.type());
        keys.forEach(key -> {
            if (key != null && contains(key)) builder.add(key);
        });
        return builder.toArray();
    }


    @Override()
    public final Iterator<K> iterator() {
        return new Iterator<K>() {
            private int ordinal = -1;
            @Override
            public boolean hasNext() {
                return ++ordinal < size();
            }
            @Override
            public K next() {
                return getKey(ordinal);
            }
        };
    }

    /**
     * Ensures the index has capacity to the size specified
     *
     * @param capacity the capacity to support
     */
    final void ensureCapacity(int capacity) {
        final int length = keys.length();
        if (length < capacity) {
            int newCapacity = length + (length >> 1);
            if (newCapacity - capacity < 0) newCapacity = capacity;
            this.keys.expand(newCapacity);
        }
    }

    @Override()
    public String toString() {
        return "Index size=" + size() + ", type=" + keys.typeCode().name();
    }

}
