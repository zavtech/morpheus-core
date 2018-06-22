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
import com.zavtech.morpheus.util.IntComparator;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

/**
 * An Index implementation designed to efficiently store integer values
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
class IndexOfInts extends IndexBase<Integer> {

    private static final long serialVersionUID = 1L;

    private Int2IntMap indexMap;

    /**
     * Constructor
     *
     * @param initialSize the initial size for this index
     */
    IndexOfInts(int initialSize) {
        super(Array.of(Integer.class, initialSize));
        this.indexMap = new Int2IntOpenHashMap(initialSize, 0.75f);
        this.indexMap.defaultReturnValue(-1);
    }

    /**
     * Constructor
     *
     * @param iterable the keys for index
     */
    IndexOfInts(Iterable<Integer> iterable) {
        super(iterable);
        this.indexMap = new Int2IntOpenHashMap(keyArray().length(), 0.75f);
        this.indexMap.defaultReturnValue(-1);
        this.keyArray().sequential().forEachValue(v -> {
            final int index = v.index();
            final int key = v.getInt();
            final int existing = indexMap.put(key, index);
            if (existing >= 0) {
                throw new IndexException("Cannot have duplicate keys in index: " + v.getValue());
            }
        });
    }

    /**
     * Constructor
     *
     * @param iterable the keys for index
     * @param parent   the parent index to initialize from
     */
    private IndexOfInts(Iterable<Integer> iterable, IndexOfInts parent) {
        super(iterable, parent);
        this.indexMap = new Int2IntOpenHashMap(keyArray().length(), 0.75f);
        this.indexMap.defaultReturnValue(-1);
        this.keyArray().sequential().forEachValue(v -> {
            final int key = v.getInt();
            final int index = parent.indexMap.get(key);
            if (index < 0) throw new IndexException("No match for key: " + v.getValue());
            final int existing = indexMap.put(key, index);
            if (existing >= 0) {
                throw new IndexException("Cannot have duplicate keys in index: " + v.getValue());
            }
        });
    }

    @Override()
    public final Index<Integer> filter(Iterable<Integer> keys) {
        return new IndexOfInts(keys, isFilter() ? (IndexOfInts) parent() : this);
    }

    @Override
    public Index<Integer> filter(Predicate<Integer> predicate) {
        final int count = size();
        final ArrayBuilder<Integer> builder = ArrayBuilder.of(count / 2, Integer.class);
        for (int i = 0; i < count; ++i) {
            final int value = keyArray().getInt(i);
            if (predicate.test(value)) {
                builder.addInt(value);
            }
        }
        final Array<Integer> filter = builder.toArray();
        return new IndexOfInts(filter, isFilter() ? (IndexOfInts) parent() : this);
    }

    @Override
    public final boolean add(Integer key) {
        if (isFilter()) {
            throw new IndexException("Cannot add keys to an filter on another index");
        } else {
            if (indexMap.containsKey(key.intValue())) {
                return false;
            } else {
                final int index = indexMap.size();
                this.ensureCapacity(index + 1);
                this.keyArray().setValue(index, key);
                this.indexMap.put((int) key, index);
                return true;
            }
        }
    }

    @Override
    public int addAll(Iterable<Integer> keys, boolean ignoreDuplicates) {
        if (isFilter()) {
            throw new IndexException("Cannot add keys to an filter on another index");
        } else {
            final int[] count = new int[1];
            keys.forEach(key -> {
                final int keyAsInt = key;
                if (!indexMap.containsKey(keyAsInt)) {
                    final int index = indexMap.size();
                    this.ensureCapacity(index + 1);
                    this.keyArray().setValue(index, keyAsInt);
                    final int existing = indexMap.put(keyAsInt, index);
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
    public final Index<Integer> copy() {
        try {
            final IndexOfInts clone = (IndexOfInts)super.copy();
            clone.indexMap = new Int2IntOpenHashMap(indexMap);
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
    public final int getIndexForKey(Integer key) {
        final int index = indexMap.get(key.intValue());
        if (index < 0) {
            throw new IndexException("No match for key in index: " + key);
        } else {
            return index;
        }
    }

    @Override
    public final boolean contains(Integer key) {
        return indexMap.containsKey(key.intValue());
    }

    @Override
    public final int replace(Integer existing, Integer replacement) {
        final int index = indexMap.remove(existing.intValue());
        if (index == -1) {
            throw new IndexException("No match key for " + existing);
        } else {
            if (indexMap.containsKey(replacement.intValue())) {
                throw new IndexException("The replacement key already exists in index " + replacement);
            } else {
                final int ordinal = getOrdinalForIndex(index);
                this.indexMap.put((int) replacement, index);
                this.keyArray().setValue(ordinal, replacement);
                return index;
            }
        }
    }

    @Override
    public final void forEachEntry(IndexConsumer<Integer> consumer) {
        final int size = size();
        for (int i = 0; i < size; ++i) {
            final Integer key = keyArray().getValue(i);
            final int index = indexMap.get(key.intValue());
            consumer.accept(key, index);
        }
    }


    @Override
    public final Index<Integer> resetOrder() {
        final Array<Integer> keys = keyArray();
        this.indexMap.forEach((key, index) -> {
            keys.setInt(index, key);
        });
        return this;
    }


    @Override
    public final Index<Integer> sort(boolean parallel, IntComparator comparator) {
        super.sort(parallel, comparator);
        if (comparator == null) {
            final Array<Integer> keys = keyArray();
            this.indexMap.forEach((key, index) -> {
                keys.setInt(index, key);
            });
        }
        return this;
   }
}
