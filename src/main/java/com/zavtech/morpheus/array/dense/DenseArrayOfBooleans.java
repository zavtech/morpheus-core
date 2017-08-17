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

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBase;
import com.zavtech.morpheus.array.ArrayBuilder;
import com.zavtech.morpheus.array.ArrayCursor;
import com.zavtech.morpheus.array.ArrayException;
import com.zavtech.morpheus.array.ArrayStyle;
import com.zavtech.morpheus.array.ArrayValue;

import it.unimi.dsi.fastutil.shorts.ShortOpenHashSet;
import it.unimi.dsi.fastutil.shorts.ShortSet;

/**
 * An Array implementation designed to hold a dense array of boolean values
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class DenseArrayOfBooleans extends ArrayBase<Boolean> {

    private static final long serialVersionUID = 1L;

    private boolean[] values;
    private boolean defaultValue;

    /**
     * Constructor
     * @param length        the length for this array
     * @param defaultValue  the default value for array
     */
    DenseArrayOfBooleans(int length, Boolean defaultValue) {
        super(Boolean.class, ArrayStyle.DENSE, false);
        this.values = new boolean[length];
        this.defaultValue = defaultValue != null ? defaultValue : false;
        Arrays.fill(values, this.defaultValue);
    }

    /**
     * Constructor
     * @param source    the source array to shallow copy
     * @param parallel  true for parallel version
     */
    private DenseArrayOfBooleans(DenseArrayOfBooleans source, boolean parallel) {
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
    public final Boolean defaultValue() {
        return defaultValue;
    }


    @Override
    public final Array<Boolean> parallel() {
        return isParallel() ? this : new DenseArrayOfBooleans(this, true);
    }


    @Override
    public final Array<Boolean> sequential() {
        return isParallel() ? new DenseArrayOfBooleans(this, false) : this;
    }


    @Override()
    public final Array<Boolean> copy() {
        try {
            final DenseArrayOfBooleans copy = (DenseArrayOfBooleans)super.clone();
            copy.defaultValue = this.defaultValue;
            copy.values = this.values.clone();
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed to copy Array: " + this, ex);
        }
    }


    @Override()
    public final Array<Boolean> copy(int[] indexes) {
        final DenseArrayOfBooleans clone = new DenseArrayOfBooleans(indexes.length, defaultValue);
        for (int i = 0; i < indexes.length; ++i) {
            clone.values[i] = this.values[indexes[i]];
        }
        return clone;
    }


    @Override()
    public final Array<Boolean> copy(int start, int end) {
        final int length = end - start;
        final DenseArrayOfBooleans clone = new DenseArrayOfBooleans(length, defaultValue);
        System.arraycopy(values, start, clone.values, 0, length);
        return clone;
    }


    @Override
    protected final Array<Boolean> sort(int start, int end, int multiplier) {
        return doSort(start, end, (i, j) -> multiplier * Boolean.compare(values[i], values[j]));
    }


    @Override
    public final int compare(int i, int j) {
        return Boolean.compare(values[i], values[j]);
    }


    @Override
    public final Array<Boolean> swap(int i, int j) {
        final boolean v1 = values[i];
        final boolean v2 = values[j];
        this.values[i] = v2;
        this.values[j] = v1;
        return this;
    }


    @Override
    public final Array<Boolean> filter(Predicate<ArrayValue<Boolean>> predicate) {
        final ArrayCursor<Boolean> cursor = cursor();
        final ArrayBuilder<Boolean> builder = ArrayBuilder.of(length(), type());
        for (int i=0; i<values.length; ++i) {
            cursor.moveTo(i);
            final boolean match = predicate.test(cursor);
            if (match) {
                builder.addBoolean(cursor.getBoolean());
            }
        }
        return builder.toArray();
    }


    @Override
    public final Array<Boolean> update(Array<Boolean> from, int[] fromIndexes, int[] toIndexes) {
        if (fromIndexes.length != toIndexes.length) {
            throw new ArrayException("The from index array must have the same length as the to index array");
        } else {
            for (int i=0; i<fromIndexes.length; ++i) {
                final int toIndex = toIndexes[i];
                final int fromIndex = fromIndexes[i];
                final boolean update = from.getBoolean(fromIndex);
                this.setBoolean(toIndex, update);
            }
        }
        return this;
    }


    @Override
    public final Array<Boolean> update(int toIndex, Array<Boolean> from, int fromIndex, int length) {
        for (int i=0; i<length; ++i) {
            final boolean update = from.getBoolean(fromIndex + i);
            this.setBoolean(toIndex + i, update);
        }
        return this;
    }


    @Override
    public final Array<Boolean> expand(int newLength) {
        if (newLength > values.length) {
            final boolean[] newValues = new boolean[newLength];
            System.arraycopy(values, 0, newValues, 0, values.length);
            Arrays.fill(newValues, values.length, newValues.length, defaultValue);
            this.values = newValues;
        }
        return this;
    }


    @Override
    public Array<Boolean> fill(Boolean value, int start, int end) {
        Arrays.fill(values, start, end, value == null ? defaultValue : value);
        return this;
    }


    @Override
    public boolean isNull(int index) {
        return false;
    }


    @Override
    public final boolean isEqualTo(int index, Boolean value) {
        return value != null && values[index] == value;
    }


    @Override
    public final boolean getBoolean(int index) {
        return values[index];
    }


    @Override
    public final Boolean getValue(int index) {
        return values[index] ? Boolean.TRUE : Boolean.FALSE;
    }


    @Override
    public final boolean setBoolean(int index, boolean value) {
        final boolean oldValue = values[index];
        this.values[index] = value;
        return oldValue;
    }


    @Override
    public final Boolean setValue(int index, Boolean value) {
        final boolean oldValue = values[index];
        if (value == null) {
            this.values[index] = defaultValue;
            return oldValue;
        } else {
            this.values[index] = value;
            return oldValue;
        }
    }


    @Override
    public int binarySearch(int start, int end, Boolean value) {
        int low = start;
        int high = end - 1;
        while (low <= high) {
            final int midIndex = (low + high) >>> 1;
            final boolean midValue = getBoolean(midIndex);
            final int result = Boolean.compare(midValue, value);
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
    public Array<Boolean> distinct(int limit) {
        final ShortSet set = new ShortOpenHashSet(limit);
        final ArrayBuilder<Boolean> builder = ArrayBuilder.of(2, Boolean.class);
        for (int i=0; i<length(); ++i) {
            final boolean value = getBoolean(i);
            if (set.add(value ? (short)1 : (short)0)) {
                builder.addBoolean(value);
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
            this.values[i] = is.readBoolean();
        }
    }

    @Override
    public final void write(ObjectOutputStream os, int[] indexes) throws IOException {
        for (int index : indexes) {
            os.writeBoolean(values[index]);
        }
    }

    /** Custom serialization */
    private void writeObject(ObjectOutputStream os) throws IOException {
        os.writeInt(values.length);
        for (boolean value : values) {
            os.writeBoolean(value);
        }
    }


    @SuppressWarnings("unchecked")
    /** Custom serialization */
    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
        final int length = is.readInt();
        this.values = new boolean[length];
        for (int i=0; i<length; ++i) {
            values[i] = is.readBoolean();
        }
    }

}
