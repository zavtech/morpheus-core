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
package com.zavtech.morpheus.array.sparse;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.function.Predicate;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBase;
import com.zavtech.morpheus.array.ArrayBuilder;
import com.zavtech.morpheus.array.ArrayCursor;
import com.zavtech.morpheus.array.ArrayException;
import com.zavtech.morpheus.array.ArrayStyle;
import com.zavtech.morpheus.array.ArrayValue;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

/**
 * An Array implementation designed to hold a sparse array of int values
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class SparseArrayOfInts extends ArrayBase<Integer> {

    private static final long serialVersionUID = 1L;

    private int length;
    private Int2IntMap values;
    private int defaultValue;

    /**
     * Constructor
     * @param length    the length for this array
     * @param defaultValue  the default value for array
     */
    SparseArrayOfInts(int length, Integer defaultValue) {
        super(Integer.class, ArrayStyle.SPARSE, false);
        this.length = length;
        this.defaultValue = defaultValue != null ? defaultValue : 0;
        this.values = new Int2IntOpenHashMap((int)Math.max(length * 0.5, 10d), 0.8f);
        this.values.defaultReturnValue(this.defaultValue);
    }

    /**
     * Constructor
     * @param source    the source array to shallow copy
     * @param parallel  true for the parallel version
     */
    private SparseArrayOfInts(SparseArrayOfInts source, boolean parallel) {
        super(source.type(), ArrayStyle.SPARSE, parallel);
        this.length = source.length;
        this.defaultValue = source.defaultValue;
        this.values = source.values;
    }


    @Override
    public final int length() {
        return length;
    }


    @Override()
    public final float loadFactor() {
        return (float)values.size() / (float)length();
    }


    @Override
    public final Integer defaultValue() {
        return defaultValue;
    }


    @Override
    public final Array<Integer> parallel() {
        return isParallel() ? this : new SparseArrayOfInts(this, true);
    }


    @Override
    public final Array<Integer> sequential() {
        return isParallel() ? new SparseArrayOfInts(this, false) : this;
    }


    @Override()
    public final Array<Integer> copy() {
        try {
            final SparseArrayOfInts copy = (SparseArrayOfInts)super.clone();
            copy.values = new Int2IntOpenHashMap(values);
            copy.defaultValue = this.defaultValue;
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed to copy Array: " + this, ex);
        }
    }


    @Override()
    public final Array<Integer> copy(int[] indexes) {
        final SparseArrayOfInts clone = new SparseArrayOfInts(indexes.length, defaultValue);
        for (int i = 0; i < indexes.length; ++i) {
            final int value = getInt(indexes[i]);
            clone.setInt(i, value);
        }
        return clone;
    }


    @Override()
    public final Array<Integer> copy(int start, int end) {
        final int length = end - start;
        final SparseArrayOfInts clone = new SparseArrayOfInts(length, defaultValue);
        for (int i=0; i<length; ++i) {
            final int value = getInt(start+i);
            if (value != defaultValue) {
                clone.setValue(i, value);
            }
        }
        return clone;
    }


    @Override
    protected final Array<Integer> sort(int start, int end, int multiplier) {
        return doSort(start, end, (i, j) -> {
            final int v1 = values.get(i);
            final int v2 = values.get(j);
            return multiplier * Integer.compare(v1, v2);
        });
    }


    @Override
    public final int compare(int i, int j) {
        return Integer.compare(values.get(i), values.get(j));
    }


    @Override
    public final Array<Integer> swap(int i, int j) {
        final int v1 = getInt(i);
        final int v2 = getInt(j);
        this.setInt(i, v2);
        this.setInt(j, v1);
        return this;
    }


    @Override
    public final Array<Integer> filter(Predicate<ArrayValue<Integer>> predicate) {
        int count = 0;
        final int length = this.length();
        final ArrayCursor<Integer> cursor = cursor();
        final Array<Integer> matches = Array.of(type(), length, loadFactor());  //todo: fix the length of this filter
        for (int i=0; i<length; ++i) {
            cursor.moveTo(i);
            final boolean match = predicate.test(cursor);
            if (match) matches.setInt(count++, cursor.getInt());
        }
        return count == length ? matches : matches.copy(0, count);
    }


    @Override
    public final Array<Integer> update(Array<Integer> from, int[] fromIndexes, int[] toIndexes) {
        if (fromIndexes.length != toIndexes.length) {
            throw new ArrayException("The from index array must have the same length as the to index array");
        } else {
            for (int i=0; i<fromIndexes.length; ++i) {
                final int toIndex = toIndexes[i];
                final int fromIndex = fromIndexes[i];
                final int update = from.getInt(fromIndex);
                this.setInt(toIndex, update);
            }
        }
        return this;
    }


    @Override
    public final Array<Integer> update(int toIndex, Array<Integer> from, int fromIndex, int length) {
        for (int i=0; i<length; ++i) {
            final int update = from.getInt(fromIndex + i);
            this.setInt(toIndex + i, update);
        }
        return this;
    }


    @Override
    public final Array<Integer> expand(int newLength) {
        this.length = newLength > length ? newLength : length;
        return this;
    }


    @Override
    public Array<Integer> fill(Integer value, int start, int end) {
        final int fillValue = value == null ? defaultValue : value;
        if (fillValue == defaultValue) {
            this.values.clear();
        } else {
            for (int i=start; i<end; ++i) {
                this.values.put(i, fillValue);
            }
        }
        return this;
    }


    @Override
    public final boolean isNull(int index) {
        return false;
    }


    @Override
    public final boolean isEqualTo(int index, Integer value) {
        return value == null ? isNull(index) : value == values.get(index);
    }


    @Override
    public final int getInt(int index) {
        this.checkBounds(index, length);
        return values.get(index);
    }

    @Override
    public final long getLong(int index) {
        this.checkBounds(index, length);
        return values.get(index);
    }

    @Override
    public final double getDouble(int index) {
        this.checkBounds(index, length);
        return values.get(index);
    }

    @Override
    public final Integer getValue(int index) {
        this.checkBounds(index, length);
        return values.get(index);
    }


    @Override
    public final int setInt(int index, int value) {
        this.checkBounds(index, length);
        final int oldValue = getInt(index);
        if (value == defaultValue) {
            this.values.remove(index);
            return oldValue;
        } else {
            this.values.put(index, value);
            return oldValue;
        }
    }


    @Override
    public final Integer setValue(int index, Integer value) {
        this.checkBounds(index, length);
        final Integer oldValue = getValue(index);
        if (value == null) {
            this.values.remove(index);
            return oldValue;
        } else {
            this.values.put((int) index, (int) value);
            return oldValue;
        }
    }


    @Override
    public final int binarySearch(int start, int end, Integer value) {
        int low = start;
        int high = end - 1;
        while (low <= high) {
            final int midIndex = (low + high) >>> 1;
            final int midValue = getInt(midIndex);
            final int result = Integer.compare(midValue, value);
            if (result < 0) {
                low = midIndex + 1;
            } else if (result > 0) {
                high = midIndex - 1;
            } else {
                return midIndex;
            }
        }
        return -(low + 1);
    }


    @Override
    public final Array<Integer> distinct(int limit) {
        final int capacity = limit < Integer.MAX_VALUE ? limit : 100;
        final IntSet set = new IntOpenHashSet(capacity);
        final ArrayBuilder<Integer> builder = ArrayBuilder.of(capacity, Integer.class);
        for (int i=0; i<length(); ++i) {
            final int value = getInt(i);
            if (set.add(value)) {
                builder.addInt(value);
                if (set.size() >= limit) {
                    break;
                }
            }
        }
        return builder.toArray();
    }


    @Override
    public final Array<Integer> cumSum() {
        final int length = length();
        final Array<Integer> result = Array.of(Integer.class, length);
        result.setInt(0, values.get(0));
        for (int i=1; i<length; ++i) {
            final int prior = result.getInt(i-1);
            final int current = values.get(i);
            result.setInt(i, prior + current);
        }
        return result;
    }


    @Override
    public final void read(ObjectInputStream is, int count) throws IOException {
        for (int i=0; i<count; ++i) {
            final int value = is.readInt();
            this.setInt(i, value);
        }
    }


    @Override
    public final void write(ObjectOutputStream os, int[] indexes) throws IOException {
        for (int index : indexes) {
            final int value = getInt(index);
            os.writeInt(value);
        }
    }

}
