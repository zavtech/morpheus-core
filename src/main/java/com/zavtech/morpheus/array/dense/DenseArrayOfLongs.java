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
package com.zavtech.morpheus.array.dense;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.function.Predicate;

import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBase;
import com.zavtech.morpheus.array.ArrayBuilder;
import com.zavtech.morpheus.array.ArrayCursor;
import com.zavtech.morpheus.array.ArrayException;
import com.zavtech.morpheus.array.ArrayStyle;
import com.zavtech.morpheus.array.ArrayValue;

/**
 * An Array implementation designed to hold a dense array of long values
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class DenseArrayOfLongs extends ArrayBase<Long> {

    private static final long serialVersionUID = 1L;

    private long[] values;
    private long defaultValue;

    /**
     * Constructor
     * @param length        the length for this array
     * @param defaultValue  the default value for array
     */
    DenseArrayOfLongs(int length, Long defaultValue) {
        super(Long.class, ArrayStyle.DENSE, false);
        this.values = new long[length];
        this.defaultValue = defaultValue != null ? defaultValue : 0L;
        Arrays.fill(values, this.defaultValue);
    }

    /**
     * Constructor
     * @param source    the source array to shallow copy
     * @param parallel  true for parallel version
     */
    private DenseArrayOfLongs(DenseArrayOfLongs source, boolean parallel) {
        super(source.type(), ArrayStyle.DENSE, parallel);
        this.values = source.values;
        this.defaultValue = source.defaultValue;
    }


    @Override
    public final int length() {
        return values.length;
    }


    @Override
    public float loadFactor() {
        return 1F;
    }


    @Override
    public final Long defaultValue() {
        return defaultValue;
    }


    @Override
    public final Array<Long> parallel() {
        return isParallel() ? this : new DenseArrayOfLongs(this, true);
    }


    @Override
    public final Array<Long> sequential() {
        return isParallel() ? new DenseArrayOfLongs(this, false) : this;
    }


    @Override()
    public final Array<Long> copy() {
        try {
            final DenseArrayOfLongs copy = (DenseArrayOfLongs)super.clone();
            copy.defaultValue = this.defaultValue;
            copy.values = this.values.clone();
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed to copy Array: " + this, ex);
        }
    }


    @Override()
    public final Array<Long> copy(int[] indexes) {
        final DenseArrayOfLongs clone = new DenseArrayOfLongs(indexes.length, defaultValue);
        for (int i = 0; i < indexes.length; ++i) {
            clone.values[i] = this.values[indexes[i]];
        }
        return clone;
    }


    @Override()
    public final Array<Long> copy(int start, int end) {
        final int length = end - start;
        final DenseArrayOfLongs clone = new DenseArrayOfLongs(length, defaultValue);
        System.arraycopy(values, start, clone.values, 0, length);
        return clone;
    }


    @Override
    protected final Array<Long> sort(int start, int end, int multiplier) {
        return doSort(start, end, (i, j) -> multiplier * Long.compare(values[i], values[j]));
    }


    @Override
    public final int compare(int i, int j) {
        return Long.compare(values[i], values[j]);
    }


    @Override
    public final Array<Long> swap(int i, int j) {
        final long v1 = values[i];
        final long v2 = values[j];
        this.values[i] = v2;
        this.values[j] = v1;
        return this;
    }


    @Override
    public final Array<Long> filter(Predicate<ArrayValue<Long>> predicate) {
        final ArrayCursor<Long> cursor = cursor();
        final ArrayBuilder<Long> builder = ArrayBuilder.of(length(), type());
        for (int i=0; i<values.length; ++i) {
            cursor.moveTo(i);
            final boolean match = predicate.test(cursor);
            if (match) {
                builder.addLong(cursor.getLong());
            }
        }
        return builder.toArray();
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
            this.setLong(toIndex + i, update);
        }
        return this;
    }


    @Override
    public final Array<Long> expand(int newLength) {
        if (newLength > values.length) {
            final long[] newValues = new long[newLength];
            System.arraycopy(values, 0, newValues, 0, values.length);
            Arrays.fill(newValues, values.length, newValues.length, defaultValue);
            this.values = newValues;
        }
        return this;
    }


    @Override
    public Array<Long> fill(Long value, int start, int end) {
        Arrays.fill(values, start, end, value == null ? defaultValue : value);
        return this;
    }


    @Override
    public boolean isNull(int index) {
        return false;
    }


    @Override
    public final boolean isEqualTo(int index, Long value) {
        return value != null && value == values[index];
    }


    @Override
    public final long getLong(int index) {
        return values[index];
    }


    @Override
    public final double getDouble(int index) {
        return values[index];
    }


    @Override
    public final Long getValue(int index) {
        return values[index];
    }


    @Override
    public final long setLong(int index, long value) {
        final long oldValue = getLong(index);
        this.values[index] = value;
        return oldValue;
    }


    @Override
    public final Long setValue(int index, Long value) {
        final Long oldValue = getValue(index);
        this.values[index] = value == null ? defaultValue : value;
        return oldValue;
    }


    @Override
    public int binarySearch(int start, int end, Long value) {
        return Arrays.binarySearch(values, start, end, value);
    }


    @Override
    public Array<Long> distinct(int limit) {
        final int capacity = limit < Integer.MAX_VALUE ? limit : 100;
        final TLongSet set = new TLongHashSet(capacity);
        final ArrayBuilder<Long> builder = ArrayBuilder.of(capacity, Long.class);
        for (int i=0; i<length(); ++i) {
            final long value = getLong(i);
            if (set.add(value)) {
                builder.addLong(value);
                if (set.size() >= limit) {
                    break;
                }
            }
        }
        return builder.toArray();
    }


    @Override
    public final void read(ObjectInputStream is, int count) throws IOException {
        for (int i=0; i<count; ++i) {
            this.values[i] = is.readLong();
        }
    }


    @Override
    public final void write(ObjectOutputStream os, int[] indexes) throws IOException {
        for (int index : indexes) {
            os.writeLong(values[index]);
        }
    }

    /** Custom serialization */
    private void writeObject(ObjectOutputStream os) throws IOException {
        os.writeInt(values.length);
        for (long value : values) {
            os.writeLong(value);
        }
    }


    @SuppressWarnings("unchecked")
    /** Custom serialization */
    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
        final int length = is.readInt();
        this.values = new long[length];
        for (int i=0; i<length; ++i) {
            values[i] = is.readLong();
        }
    }


}
