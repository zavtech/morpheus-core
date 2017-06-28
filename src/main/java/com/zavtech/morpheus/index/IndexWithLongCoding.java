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
import com.zavtech.morpheus.array.coding.LongCoding;
import com.zavtech.morpheus.array.coding.WithLongCoding;

import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongIntHashMap;

/**
 * An Index implementation designed to efficiently store Object values that can be coded as a long value
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
class IndexWithLongCoding<T> extends IndexBase<T> implements WithLongCoding<T> {

    private static final long serialVersionUID = 1L;

    private TLongIntMap indexMap;
    private LongCoding<T> coding;

    /**
     * Constructor for empty index with initial capacity
     * @param type      the array type
     * @param coding    the coding for this index
     * @param capacity  the initial capacity for this index
     */
    IndexWithLongCoding(Class<T> type, LongCoding<T> coding, int capacity) {
        super(Array.of(type, capacity));
        this.coding = coding;
        this.indexMap = new TLongIntHashMap(capacity, 0.75f, -1L, -1);
    }

    /**
     * Constructor
     * @param iterable  the keys for this index
     * @param coding    the coding for this index
     */
    IndexWithLongCoding(Iterable<T> iterable, LongCoding<T> coding) {
        super(iterable);
        this.coding = coding;
        this.indexMap = new TLongIntHashMap(keyArray().length(), 0.75f, -1L, -1);
        this.keyArray().sequential().forEachValue(v -> {
            final int index = v.index();
            final long code = v.getLong();
            final int existing = indexMap.put(code, index);
            if (existing >= 0) {
                throw new IndexException("Cannot have duplicate keys in index: " + v.getValue());
            }
        });
    }

    /**
     * Constructor
     * @param iterable  the keys for index
     * @param coding    the coding for this index
     * @param parent    the parent index to initialize from
     */
    private IndexWithLongCoding(Iterable<T> iterable, LongCoding<T> coding, IndexWithLongCoding<T> parent) {
        super(iterable, parent);
        this.coding = coding;
        this.indexMap = new TLongIntHashMap(keyArray().length(), 0.75f, -1L, -1);
        this.keyArray().sequential().forEachValue(v -> {
            final long code = v.getLong();
            final int index = parent.indexMap.get(code);
            if (index < 0) throw new IndexException("No match for key: " + v.getValue());
            final int existing = indexMap.put(code, index);
            if (existing >= 0) {
                throw new IndexException("Cannot have duplicate keys in index: " + v.getValue());
            }
        });
    }


    @Override
    public final LongCoding<T> getCoding() {
        return coding;
    }


    @Override()
    public final Index<T> filter(Iterable<T> keys) {
        return new IndexWithLongCoding<>(keys, coding, isFilter() ? (IndexWithLongCoding<T>)parent() : this);
    }

    @Override
    public final Index<T> filter(Predicate<T> predicate) {
        final int count = size();
        final Class<T> typeClass = keyArray().type();
        final ArrayBuilder<T> builder = ArrayBuilder.of(count / 2, typeClass);
        for (int i=0; i<count; ++i) {
            final T key = keyArray().getValue(i);
            if (predicate.test(key)) {
                builder.add(key);
            }
        }
        final Array<T> filter = builder.toArray();
        return new IndexWithLongCoding<>(filter, coding, isFilter() ? (IndexWithLongCoding<T>)parent() : this);
    }

    @Override
    public final boolean add(T key) {
        if (isFilter()) {
            throw new IndexException("Cannot add keys to a filter on another index");
        } else {
            final long code = coding.getCode(key);
            if (indexMap.containsKey(code)) {
                return false;
            } else {
                final int index = indexMap.size();
                this.ensureCapacity(index + 1);
                this.keyArray().setValue(index, key);
                this.indexMap.put(code, index);
                return true;
            }
        }
    }

    @Override
    public final int addAll(Iterable<T> keys, boolean ignoreDuplicates) {
        if (isFilter()) {
            throw new IndexException("Cannot add keys to a filter on another index");
        } else {
            final int[] count = new int[1];
            keys.forEach(key -> {
                final long code = coding.getCode(key);
                if (!indexMap.containsKey(code)) {
                    final int index = indexMap.size();
                    this.ensureCapacity(index + 1);
                    this.keyArray().setValue(index, key);
                    final int existing = indexMap.put(code, index);
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
    @SuppressWarnings("unchecked")
    public final Index<T> copy() {
        try {
            final IndexWithLongCoding<T> clone = (IndexWithLongCoding<T>)super.copy();
            clone.indexMap = new TLongIntHashMap(indexMap);
            clone.coding = coding;
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
    public final int getIndexForKey(T key) {
        final long code = coding.getCode(key);
        final int index = indexMap.get(code);
        if (index < 0) {
            throw new IndexException("No match for key in index: " + key);
        } else {
            return index;
        }
    }

    @Override
    public final boolean contains(T key) {
        final long code = coding.getCode(key);
        return indexMap.containsKey(code);
    }

    @Override
    public final int replace(T existing, T replacement) {
        final long code = coding.getCode(existing);
        final int index = indexMap.remove(code);
        if (index == -1) {
            throw new IndexException("No match key for " + existing);
        } else {
            final long replacementCode = coding.getCode(replacement);
            if (indexMap.containsKey(replacementCode)) {
                throw new IndexException("The replacement key already exists in index " + replacement);
            } else {
                final int ordinal = getOrdinalForIndex(index);
                this.indexMap.put(replacementCode, index);
                this.keyArray().setValue(ordinal, replacement);
                return index;
            }
        }
    }

    @Override()
    public final void forEachEntry(IndexConsumer<T> consumer) {
        final int size = size();
        for (int i=0; i<size; ++i) {
            final T key = keyArray().getValue(i);
            final long code = coding.getCode(key);
            final int index = indexMap.get(code);
            consumer.accept(key, index);
        }
    }


}
