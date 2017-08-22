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

import it.unimi.dsi.fastutil.doubles.Double2IntMap;
import it.unimi.dsi.fastutil.doubles.Double2IntOpenHashMap;

/**
 * An Index implementation designed to efficiently store Double values
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
class IndexOfDoubles extends IndexBase<Double> {

    private static final long serialVersionUID = 1L;

    private Double2IntMap indexMap;

    /**
     * Constructor
     * @param initialSize   the initial size for this index
     */
    IndexOfDoubles(int initialSize) {
        super(Array.of(Double.class, initialSize));
        this.indexMap = new Double2IntOpenHashMap(initialSize, 0.75f);
        this.indexMap.defaultReturnValue(-1);
    }

    /**
     * Constructor
     * @param iterable      the keys for index
     */
    IndexOfDoubles(Iterable<Double> iterable) {
        super(iterable);
        this.indexMap = new Double2IntOpenHashMap(keyArray().length(), 0.75f);
        this.indexMap.defaultReturnValue(-1);
        this.keyArray().sequential().forEachValue(v -> {
            final int index = v.index();
            final double key = v.getDouble();
            final int existing = indexMap.put(key, index);
            if (existing >= 0) {
                throw new IndexException("Cannot have duplicate keys in index: " + v.getValue());
            }
        });
    }

    /**
     * Constructor
     * @param iterable      the keys for index
     * @param parent    the parent index to initialize from
     */
    private IndexOfDoubles(Iterable<Double> iterable, IndexOfDoubles parent) {
        super(iterable, parent);
        this.indexMap = new Double2IntOpenHashMap(keyArray().length(), 0.75f);
        this.indexMap.defaultReturnValue(-1);
        this.keyArray().sequential().forEachValue(v -> {
            final double key = v.getDouble();
            final int index = parent.indexMap.get(key);
            if (index < 0) throw new IndexException("No match for key: " + v.getValue());
            final int existing = indexMap.put(key, index);
            if (existing >= 0) {
                throw new IndexException("Cannot have duplicate keys in index: " + v.getValue());
            }
        });
    }

    @Override()
    public final Index<Double> filter(Iterable<Double> keys) {
        return new IndexOfDoubles(keys, isFilter() ? (IndexOfDoubles)parent() : this);
    }

    @Override
    public Index<Double> filter(Predicate<Double> predicate) {
        final int count = size();
        final ArrayBuilder<Double> builder = ArrayBuilder.of(count / 2, Double.class);
        for (int i=0; i<count; ++i) {
            final double value = keyArray().getDouble(i);
            if (predicate.test(value)) {
                builder.addDouble(value);
            }
        }
        final Array<Double> filter = builder.toArray();
        return new IndexOfDoubles(filter, isFilter() ? (IndexOfDoubles)parent() : this);
    }

    @Override
    public final boolean add(Double key) {
        if (isFilter()) {
            throw new IndexException("Cannot add keys to an filter on another index");
        } else {
            if (indexMap.containsKey(key)) {
                return false;
            } else {
                final int index = indexMap.size();
                this.ensureCapacity(index + 1);
                this.keyArray().setValue(index, key);
                this.indexMap.put((double) key, index);
                return true;
            }
        }
    }

    @Override
    public final int addAll(Iterable<Double> keys, boolean ignoreDuplicates) {
        if (isFilter()) {
            throw new IndexException("Cannot add keys to an filter on another index");
        } else {
            final int[] count = new int[1];
            keys.forEach(key -> {
                final double keyAsDouble = key;
                if (!indexMap.containsKey(keyAsDouble)) {
                    final int index = indexMap.size();
                    this.ensureCapacity(index + 1);
                    this.keyArray().setDouble(index, keyAsDouble);
                    final int existing = indexMap.put(keyAsDouble, index);
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
    public final Index<Double> copy() {
        try {
            final IndexOfDoubles clone = (IndexOfDoubles)super.copy();
            clone.indexMap = new Double2IntOpenHashMap(indexMap);
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
    public final int getIndexForKey(Double key) {
        final int index = indexMap.get(key);
        if (index < 0) {
            throw new IndexException("No match for key in index: " + key);
        } else {
            return index;
        }
    }

    @Override
    public final boolean contains(Double key) {
        return indexMap.containsKey(key);
    }

    @Override
    public final int replace(Double existing, Double replacement) {
        final int index = indexMap.remove(existing);
        if (index == -1) {
            throw new IndexException("No match key for " + existing);
        } else {
            if (indexMap.containsKey(replacement)) {
                throw new IndexException("The replacement key already exists in index " + replacement);
            } else {
                final int ordinal = getOrdinalForIndex(index);
                this.indexMap.put((double) replacement, index);
                this.keyArray().setValue(ordinal, replacement);
                return index;
            }
        }
    }

    @Override()
    public final void forEachEntry(IndexConsumer<Double> consumer) {
        final int size = size();
        for (int i=0; i<size; ++i) {
            final Double key = keyArray().getValue(i);
            final int index = indexMap.get(key);
            consumer.accept(key, index);
        }
    }

}
