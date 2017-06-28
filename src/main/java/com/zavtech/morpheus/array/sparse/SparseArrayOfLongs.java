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

import com.zavtech.morpheus.array.ArrayException;
import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBase;
import com.zavtech.morpheus.array.ArrayStyle;

import gnu.trove.map.TIntLongMap;
import gnu.trove.map.hash.TIntLongHashMap;

/**
 * An Array implementation designed to hold a sparse array of long values
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class SparseArrayOfLongs extends ArrayBase<Long> {

    private static final long serialVersionUID = 1L;

    private int length;
    private TIntLongMap values;
    private long defaultValue;

    /**
     * Constructor
     * @param length    the length for this array
     * @param defaultValue  the default value for array
     */
    SparseArrayOfLongs(int length, Long defaultValue) {
        super(Long.class, ArrayStyle.SPARSE, false);
        this.length = length;
        this.defaultValue = defaultValue != null ? defaultValue : 0L;
        this.values = new TIntLongHashMap((int)Math.max(length * 0.5, 10d), 0.8f, -1, this.defaultValue);
    }

    /**
     * Constructor
     * @param source    the source array to shallow copy
     * @param parallel  true for the parallel version
     */
    private SparseArrayOfLongs(SparseArrayOfLongs source, boolean parallel) {
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
    public final Long defaultValue() {
        return defaultValue;
    }


    @Override
    public final Array<Long> parallel() {
        return isParallel() ? this : new SparseArrayOfLongs(this, true);
    }


    @Override
    public final Array<Long> sequential() {
        return isParallel() ? new SparseArrayOfLongs(this, false) : this;
    }


    @Override()
    public final Array<Long> copy() {
        try {
            final SparseArrayOfLongs copy = (SparseArrayOfLongs)super.clone();
            copy.values = new TIntLongHashMap(values);
            copy.defaultValue = this.defaultValue;
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed to copy Array: " + this, ex);
        }
    }


    @Override()
    public final Array<Long> copy(int[] indexes) {
        final SparseArrayOfLongs clone = new SparseArrayOfLongs(indexes.length, defaultValue);
        for (int i = 0; i < indexes.length; ++i) {
            final long value = getLong(indexes[i]);
            clone.setLong(i, value);
        }
        return clone;
    }


    @Override()
    public final Array<Long> copy(int start, int end) {
        final int length = end - start;
        final SparseArrayOfLongs clone = new SparseArrayOfLongs(length, defaultValue);
        for (int i=0; i<length; ++i) {
            final long value = getLong(start+i);
            if (value != defaultValue) {
                clone.setLong(i, value);
            }
        }
        return clone;
    }


    @Override
    protected final Array<Long> sort(int start, int end, int multiplier) {
        return doSort(start, end, (i, j) -> {
            final long v1 = values.get(i);
            final long v2 = values.get(j);
            return multiplier * Long.compare(v1, v2);
        });
    }


    @Override
    public final int compare(int i, int j) {
        return Long.compare(values.get(i), values.get(j));
    }


    @Override
    public final Array<Long> swap(int i, int j) {
        final long v1 = getLong(i);
        final long v2 = getLong(j);
        this.setLong(i, v2);
        this.setLong(j, v1);
        return this;
    }


    @Override
    public final Array<Long> filter(Predicate<Long> predicate) {
        int count = 0;
        final int length = this.length();
        final Array<Long> matches = Array.of(type(), length, loadFactor());  //todo: fix the length of this filter
        for (int i=0; i<length; ++i) {
            final long value = getLong(i);
            final boolean match = predicate.test(value);
            if (match) matches.setLong(count++, value);
        }
        return count == length ? matches : matches.copy(0, count);
    }


    @Override
    public final Array<Long> update(Array<Long> from, int[] fromIndexes, int[] toIndexes) {
        if (fromIndexes.length != toIndexes.length) {
            throw new ArrayException("The from index array must have the same length as the to index array");
        } else {
            for (int i=0; i<fromIndexes.length; ++i) {
                final int toIndex = toIndexes[i];
                final int fromIndex = fromIndexes[i];
                final long update = from.getLong(fromIndex);
                this.setLong(toIndex, update);
            }
        }
        return this;
    }


    @Override
    public final Array<Long> update(int toIndex, Array<Long> from, int fromIndex, int length) {
        for (int i=0; i<length; ++i) {
            final long update = from.getLong(fromIndex + i);
            this.expand(toIndex + i);
            this.setLong(toIndex + i, update);
        }
        return this;
    }


    @Override
    public final Array<Long> expand(int newLength) {
        this.length = newLength > length ? newLength : length;
        return this;
    }


    @Override
    public final Array<Long> fill(Long value) {
        return fill(value, 0, length());
    }


    @Override
    public Array<Long> fill(Long value, int start, int end) {
        final long fillValue = value == null ? 0L : value;
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
    public final boolean isEqualTo(int index, Long value) {
        return value == null ? isNull(index) : value == values.get(index);
    }


    @Override
    public final long getLong(int index) {
        this.checkBounds(index, length);
        return values.get(index);
    }


    @Override
    public double getDouble(int index) {
        this.checkBounds(index, length);
        return values.get(index);
    }


    @Override
    public final Long getValue(int index) {
        this.checkBounds(index, length);
        return values.get(index);
    }


    @Override
    public final long setLong(int index, long value) {
        this.checkBounds(index, length);
        final long oldValue = getLong(index);
        if (value == defaultValue) {
            this.values.remove(index);
            return oldValue;
        } else {
            this.values.put(index, value);
            return oldValue;
        }
    }


    @Override
    public final Long setValue(int index, Long value) {
        this.checkBounds(index, length);
        final Long oldValue = getValue(index);
        if (value == null) {
            this.values.remove(index);
            return oldValue;
        } else {
            this.values.put(index,value);
            return oldValue;
        }
    }


    @Override
    public int binarySearch(int start, int end, Long value) {
        int low = start;
        int high = end - 1;
        while (low <= high) {
            final int midIndex = (low + high) >>> 1;
            final long midValue = getLong(midIndex);
            final int result = Long.compare(midValue, value);
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
    public final void read(ObjectInputStream is, int count) throws IOException {
        for (int i=0; i<count; ++i) {
            final long value = is.readLong();
            this.setLong(i, value);
        }
    }


    @Override
    public final void write(ObjectOutputStream os, int[] indexes) throws IOException {
        for (int index : indexes) {
            final long value = getLong(index);
            os.writeLong(value);
        }
    }
}
