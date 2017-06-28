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

import java.util.function.Predicate;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBuilder;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

/**
 * An Index implementation designed to store any object type.
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
class IndexOfObjects<K> extends IndexBase<K> {

    private static final long serialVersionUID = 1L;

    private TObjectIntMap<K> indexMap;

    /**
     * Constructor
     * @param type      the element type
     * @param initialSize   the initial size for this index
     */
    IndexOfObjects(Class<K> type, int initialSize) {
        super(Array.of(type, initialSize));
        this.indexMap = new TObjectIntHashMap<>(initialSize, 0.75f, -1);
    }

    /**
     * Constructor
     * @param iterable      the keys for index
     */
    IndexOfObjects(Iterable<K> iterable) {
        super(iterable);
        this.indexMap = new TObjectIntHashMap<>(keyArray().length(), 0.75f, -1);
        this.keyArray().sequential().forEachValue(v -> {
            final int index = v.index();
            final K key = v.getValue();
            final int existing = indexMap.put(key, index);
            if (existing >= 0) {
                throw new IndexException("Cannot have duplicate keys in index: " + v.getValue());
            }
        });
    }

    /**
     * Constructor
     * @param iterable  the keys for index
     * @param parent    the parent index to initialize from
     */
    private IndexOfObjects(Iterable<K> iterable, IndexOfObjects<K> parent) {
        super(iterable, parent);
        this.indexMap = new TObjectIntHashMap<>(keyArray().length(), 0.75f, -1);
        this.keyArray().sequential().forEachValue(v -> {
            final K key = v.getValue();
            final int index = parent.indexMap.get(key);
            if (index < 0) throw new IndexException("No match for key: " + v.getValue());
            final int existing = indexMap.put(key, index);
            if (existing >= 0) {
                throw new IndexException("Cannot have duplicate keys in index: " + v.getValue());
            }
        });
    }

    @Override()
    public final Index<K> filter(Iterable<K> keys) {
        return new IndexOfObjects<>(keys, isFilter() ? (IndexOfObjects<K>)parent() : this);
    }

    @Override
    public Index<K> filter(Predicate<K> predicate) {
        final int count = size();
        final Class<K> type = type();
        final ArrayBuilder<K> builder = ArrayBuilder.of(count / 2, type);
        for (int i=0; i<count; ++i) {
            final K value = keyArray().getValue(i);
            if (predicate.test(value)) {
                builder.add(value);
            }
        }
        final Array<K> filter = builder.toArray();
        return new IndexOfObjects<>(filter, isFilter() ? (IndexOfObjects<K>)parent() : this);
    }

    @Override
    public final boolean add(K key) {
        if (isFilter()) {
            throw new IndexException("Cannot add keys to an filter on another index");
        } else {
            if (indexMap.containsKey(key)) {
                return false;
            } else {
                final int index = indexMap.size();
                this.ensureCapacity(index + 1);
                this.keyArray().setValue(index, key);
                this.indexMap.put(key, index);
                return true;
            }
        }
    }

    @Override
    public int addAll(Iterable<K> keys, boolean ignoreDuplicates) {
        if (isFilter()) {
            throw new IndexException("Cannot add keys to an filter on another index");
        } else {
            final int[] count = new int[1];
            keys.forEach(key -> {
                if (!indexMap.containsKey(key)) {
                    final int index = indexMap.size();
                    this.ensureCapacity(index + 1);
                    this.keyArray().setValue(index, key);
                    final int existing = indexMap.put(key, index);
                    if (!ignoreDuplicates && existing >= 0) {
                        throw new IndexException("Attempt to add duplicate key to index: " + key);
                    }
                    ++count[0];
                }
            });
            return count[0];
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public final Index<K> copy() {
        try {
            final IndexOfObjects<K> clone = (IndexOfObjects<K>)super.copy();
            clone.indexMap = new TObjectIntHashMap<>(indexMap);
            return clone;
        } catch (Exception ex) {
            throw new IndexException("Failed to clone index", ex);
        }
    }

    @Override
    public final int size() {
        return indexMap.size();
    }

    @Override
    public final int getIndexForKey(K key) {
        final int index = indexMap.get(key);
        if (index < 0) {
            throw new IndexException("No match for key in index: " + key);
        } else {
            return index;
        }
    }

    @Override
    public final boolean contains(K key) {
        return indexMap.containsKey(key);
    }

    @Override
    public final int replace(K existing, K replacement) {
        final int index = indexMap.remove(existing);
        if (index == -1) {
            throw new IndexException("No match key for " + existing);
        } else {
            if (indexMap.containsKey(replacement)) {
                throw new IndexException("The replacement key already exists in index " + replacement);
            } else {
                final int ordinal = getOrdinalForIndex(index);
                this.indexMap.put(replacement, index);
                this.keyArray().setValue(ordinal, replacement);
                return index;
            }
        }
    }

    @Override()
    public final void forEachEntry(IndexConsumer<K> consumer) {
        final int size = size();
        for (int i=0; i<size; ++i) {
            final K key = keyArray().getValue(i);
            final int index = indexMap.get(key);
            consumer.accept(key, index);
        }
    }

}
