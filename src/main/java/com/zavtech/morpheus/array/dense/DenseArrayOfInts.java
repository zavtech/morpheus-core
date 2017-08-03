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

import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBase;
import com.zavtech.morpheus.array.ArrayBuilder;
import com.zavtech.morpheus.array.ArrayCursor;
import com.zavtech.morpheus.array.ArrayException;
import com.zavtech.morpheus.array.ArrayStyle;
import com.zavtech.morpheus.array.ArrayValue;

/**
 * An Array implementation designed to hold a dense array of int values
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class DenseArrayOfInts extends ArrayBase<Integer> {

    private static final long serialVersionUID = 1L;

    private int[] values;
    private int defaultValue;

    /**
     * Constructor
     * @param length        the length for this array
     * @param defaultValue  the default value for array
     */
    DenseArrayOfInts(int length, Integer defaultValue) {
        super(Integer.class, ArrayStyle.DENSE, false);
        this.values = new int[length];
        this.defaultValue = defaultValue != null ? defaultValue : 0;
        Arrays.fill(values, this.defaultValue);
    }

    /**
     * Constructor
     * @param source    the source array to shallow copy
     * @param parallel  true for parallel version
     */
    private DenseArrayOfInts(DenseArrayOfInts source, boolean parallel) {
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
    public final Integer defaultValue() {
        return defaultValue;
    }


    @Override
    public final Array<Integer> parallel() {
        return isParallel() ? this : new DenseArrayOfInts(this, true);
    }


    @Override
    public final Array<Integer> sequential() {
        return isParallel() ? new DenseArrayOfInts(this, false) : this;
    }


    @Override()
    public final Array<Integer> copy() {
        try {
            final DenseArrayOfInts copy = (DenseArrayOfInts)super.clone();
            copy.defaultValue = this.defaultValue;
            copy.values = this.values.clone();
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed to copy Array: " + this, ex);
        }
    }


    @Override()
    public final Array<Integer> copy(int[] indexes) {
        final DenseArrayOfInts clone = new DenseArrayOfInts(indexes.length, defaultValue);
        for (int i = 0; i < indexes.length; ++i) {
            clone.values[i] = this.values[indexes[i]];
        }
        return clone;
    }


    @Override()
    public final Array<Integer> copy(int start, int end) {
        final int length = end - start;
        final DenseArrayOfInts clone = new DenseArrayOfInts(length, defaultValue);
        System.arraycopy(values, start, clone.values, 0, length);
        return clone;
    }


    @Override
    protected final Array<Integer> sort(int start, int end, int multiplier) {
        return doSort(start, end, (i, j) -> multiplier * Integer.compare(values[i], values[j]));
    }


    @Override
    public final int compare(int i, int j) {
        return Integer.compare(values[i], values[j]);
    }


    @Override
    public final Array<Integer> swap(int i, int j) {
        final int v1 = values[i];
        final int v2 = values[j];
        this.values[i] = v2;
        this.values[j] = v1;
        return this;
    }


    @Override
    public final Array<Integer> filter(Predicate<ArrayValue<Integer>> predicate) {
        final ArrayCursor<Integer> cursor = cursor();
        final ArrayBuilder<Integer> builder = ArrayBuilder.of(length(), type());
        for (int i=0; i<values.length; ++i) {
            cursor.moveTo(i);
            final boolean match = predicate.test(cursor);
            if (match) {
                builder.addInt(cursor.getInt());
            }
        }
        return builder.toArray();
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
        if (newLength > values.length) {
            final int[] newValues = new int[newLength];
            System.arraycopy(values, 0, newValues, 0, values.length);
            Arrays.fill(newValues, values.length, newValues.length, defaultValue);
            this.values = newValues;
        }
        return this;
    }


    @Override
    public Array<Integer> fill(Integer value, int start, int end) {
        Arrays.fill(values, start, end, value == null ? defaultValue : value);
        return this;
    }

    @Override
    public boolean isNull(int index) {
        return false;
    }


    @Override
    public final boolean isEqualTo(int index, Integer value) {
        return value != null && value == values[index];
    }


    @Override
    public final int getInt(int index) {
        return values[index];
    }


    @Override
    public final double getDouble(int index) {
        return values[index];
    }


    @Override
    public final Integer getValue(int index) {
        return values[index];
    }


    @Override
    public final int setInt(int index, int value) {
        final int oldValue = getInt(index);
        this.values[index] = value;
        return oldValue;
    }


    @Override
    public final Integer setValue(int index, Integer value) {
        final Integer oldValue = getValue(index);
        if (value == null) {
            this.values[index] = defaultValue;
            return oldValue;
        } else {
            this.values[index] = value;
            return oldValue;
        }
    }


    @Override
    public final int binarySearch(int start, int end, Integer value) {
        return Arrays.binarySearch(values, start, end, value);
    }


    @Override
    public final Array<Integer> distinct(int limit) {
        final int capacity = limit < Integer.MAX_VALUE ? limit : 100;
        final TIntSet set = new TIntHashSet(capacity);
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
        result.setInt(0, values[0]);
        for (int i=1; i<length; ++i) {
            final int prior = result.getInt(i-1);
            final int current = values[i];
            result.setInt(i, prior + current);
        }
        return result;
    }


    @Override
    public final void read(ObjectInputStream is, int count) throws IOException {
        for (int i=0; i<count; ++i) {
            this.values[i] = is.readInt();
        }
    }


    @Override
    public final void write(ObjectOutputStream os, int[] indexes) throws IOException {
        for (int index : indexes) {
            os.writeInt(values[index]);
        }
    }

    /** Custom serialization */
    private void writeObject(ObjectOutputStream os) throws IOException {
        os.writeInt(values.length);
        for (int value : values) {
            os.writeInt(value);
        }
    }


    @SuppressWarnings("unchecked")
    /** Custom serialization */
    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
        final int length = is.readInt();
        this.values = new int[length];
        for (int i=0; i<length; ++i) {
            values[i] = is.readInt();
        }
    }

}
