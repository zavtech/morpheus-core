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
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

/**
 * An Index implementation designed to efficiently store String values
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
class IndexOfStrings extends IndexBase<String> {

    private static final long serialVersionUID = 1L;

    private Object2IntMap<String> indexMap;

    /**
     * Constructor
     * @param initialSize   the initial size for this index
     */
    IndexOfStrings(int initialSize) {
        super(Array.of(String.class, initialSize));
        this.indexMap = new Object2IntOpenHashMap<>(initialSize, 0.75f);
        this.indexMap.defaultReturnValue(-1);
    }

    /**
     * Constructor
     * @param iterable  the keys for index
     */
    IndexOfStrings(Iterable<String> iterable) {
        super(iterable);
        this.indexMap = new Object2IntOpenHashMap<>(keyArray().length(), 0.75f);
        this.indexMap.defaultReturnValue(-1);
        this.keyArray().sequential().forEachValue(v -> {
            final int index = v.index();
            final String key = v.getValue();
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
    private IndexOfStrings(Iterable<String> iterable, IndexOfStrings parent) {
        super(iterable, parent);
        this.indexMap = new Object2IntOpenHashMap<>(keyArray().length(), 0.75f);
        this.indexMap.defaultReturnValue(-1);
        this.keyArray().sequential().forEachValue(v -> {
            final String key = v.getValue();
            final int index = parent.indexMap.getInt(key);
            if (index < 0) throw new IndexException("No match for key: " + v.getValue());
            final int existing = indexMap.put(key, index);
            if (existing >= 0) {
                throw new IndexException("Cannot have duplicate keys in index: " + v.getValue());
            }
        });
    }

    @Override()
    public final Index<String> filter(Iterable<String> keys) {
        return new IndexOfStrings(keys, isFilter() ? (IndexOfStrings)parent() : this);
    }

    @Override
    public final Index<String> filter(Predicate<String> predicate) {
        final int count = size();
        final ArrayBuilder<String> builder = ArrayBuilder.of(count / 2, String.class);
        for (int i=0; i<count; ++i) {
            final String value = keyArray().getValue(i);
            if (predicate.test(value)) {
                builder.add(value);
            }
        }
        final Array<String> filter = builder.toArray();
        return new IndexOfStrings(filter, isFilter() ? (IndexOfStrings)parent() : this);
    }

    @Override
    public final boolean add(String key) {
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
    public final int addAll(Iterable<String> keys, boolean ignoreDuplicates) {
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
                    count[0]++;
                }
            });
            return count[0];
        }
    }

    @Override
    public final Index<String> copy() {
        try {
            final IndexOfStrings clone = (IndexOfStrings)super.copy();
            clone.indexMap = new Object2IntOpenHashMap<>(indexMap);
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
    public final int getIndexForKey(String key) {
        final int index = indexMap.getInt(key);
        if (index < 0) {
            throw new IndexException("No match for key in index: " + key);
        } else {
            return index;
        }
    }

    @Override
    public final boolean contains(String key) {
        return indexMap.containsKey(key);
    }

    @Override
    public final int replace(String existing, String replacement) {
        final int index = indexMap.removeInt(existing);
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
    public final void forEachEntry(IndexConsumer<String> consumer) {
        final int size = size();
        for (int i=0; i<size; ++i) {
            final String key = keyArray().getValue(i);
            final int index = indexMap.getInt(key);
            consumer.accept(key, index);
        }
    }

}
