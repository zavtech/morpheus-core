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
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongIntHashMap;

/**
 * An Index implementation designed to efficiently store long values
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
class IndexOfLongs extends IndexBase<Long> {

    private static final long serialVersionUID = 1L;

    private TLongIntMap indexMap;

    /**
     * Constructor
     * @param initialSize   the initial size for this index
     */
    IndexOfLongs(int initialSize) {
        super(Array.of(Long.class, initialSize));
        this.indexMap = new TLongIntHashMap(initialSize, 0.75f, -1, -1);
    }

    /**
     * Constructor
     * @param iterable      the keys for index
     */
    IndexOfLongs(Iterable<Long> iterable) {
        super(iterable);
        this.indexMap = new TLongIntHashMap(keyArray().length(), 0.75f, -1, -1);
        this.keyArray().sequential().forEachValue(v -> {
            final int index = v.index();
            final long key = v.getLong();
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
    private IndexOfLongs(Iterable<Long> iterable, IndexOfLongs parent) {
        super(iterable, parent);
        this.indexMap = new TLongIntHashMap(keyArray().length(), 0.75f, -1, -1);
        this.keyArray().sequential().forEachValue(v -> {
            final long key = v.getLong();
            final int index = parent.indexMap.get(key);
            if (index < 0) throw new IndexException("No match for key: " + v.getValue());
            final int existing = indexMap.put(key, index);
            if (existing >= 0) {
                throw new IndexException("Cannot have duplicate keys in index: " + v.getValue());
            }
        });
    }

    @Override()
    public final Index<Long> filter(Iterable<Long> keys) {
        return new IndexOfLongs(keys, isFilter() ? (IndexOfLongs)parent() : this);
    }

    @Override
    public final Index<Long> filter(Predicate<Long> predicate) {
        final int count = size();
        final ArrayBuilder<Long> builder = ArrayBuilder.of(count / 2, Long.class);
        for (int i=0; i<count; ++i) {
            final long value = keyArray().getLong(i);
            if (predicate.test(value)) {
                builder.addLong(value);
            }
        }
        final Array<Long> filter = builder.toArray();
        return new IndexOfLongs(filter, isFilter() ? (IndexOfLongs)parent() : this);
    }

    @Override
    public final boolean add(Long key) {
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
    public final int addAll(Iterable<Long> keys, boolean ignoreDuplicates) {
        if (isFilter()) {
            throw new IndexException("Cannot add keys to an filter on another index");
        } else {
            final int[] count = new int[1];
            keys.forEach(key -> {
                final long keyAsLong = key;
                if (!indexMap.containsKey(keyAsLong)) {
                    final int index = indexMap.size();
                    this.ensureCapacity(index + 1);
                    this.keyArray().setValue(index, keyAsLong);
                    final int existing = indexMap.put(keyAsLong, index);
                    if (!ignoreDuplicates && existing >= 0) {
                        throw new IndexException("Attempt to add duplicate key to index: " + key);
                    }
                    count[0]++;
                }
            });
            return count[0];
        }
    }

    @Override
    public final Index<Long> copy() {
        try {
            final IndexOfLongs clone = (IndexOfLongs)super.copy();
            clone.indexMap = new TLongIntHashMap(indexMap);
            return clone;
        } catch (Exception ex) {
            throw new IndexException("Failed to clone index", ex);
        }
    }

    @Override
    public int size() {
        return indexMap.size();
    }

    @Override
    public int getIndexForKey(Long key) {
        final int index = indexMap.get(key);
        if (index < 0) {
            throw new IndexException("No match for key in index: " + key);
        } else {
            return index;
        }
    }

    @Override
    public boolean contains(Long key) {
        return indexMap.containsKey(key);
    }

    @Override
    public final int replace(Long existing, Long replacement) {
        final int index = indexMap.remove(existing);
        if (index == -1) {
            throw new IndexException("No match for key: " + existing);
        } else {
            if (indexMap.containsKey(replacement)) {
                throw new IndexException("The replacement key already exists: " + replacement);
            } else {
                final int ordinal = getOrdinalForIndex(index);
                this.indexMap.put(replacement, index);
                this.keyArray().setValue(ordinal, replacement);
                return index;
            }
        }
    }

    @Override()
    public final void forEachEntry(IndexConsumer<Long> consumer) {
        final int size = size();
        for (int i=0; i<size; ++i) {
            final Long key = keyArray().getValue(i);
            final int index = indexMap.get(key);
            consumer.accept(key, index);
        }
    }

}
