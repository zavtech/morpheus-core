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

import com.zavtech.morpheus.array.ArrayBuilder;
import com.zavtech.morpheus.array.ArrayCursor;
import com.zavtech.morpheus.array.ArrayException;
import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBase;
import com.zavtech.morpheus.array.ArrayStyle;
import com.zavtech.morpheus.array.ArrayValue;

/**
 * An Array implementation designed to hold a dense array of Object values
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class DenseArrayOfObjects<T> extends ArrayBase<T> {

    private static final long serialVersionUID = 1L;

    private Object[] values;
    private T defaultValue;

    /**
     * Constructor
     * @param type          the type for this array
     * @param length        the length for this array
     * @param defaultValue  the default value for array
     */
    DenseArrayOfObjects(Class<T> type, int length, T defaultValue) {
        super(type, ArrayStyle.DENSE, false);
        this.values = new Object[length];
        this.defaultValue = defaultValue;
        Arrays.fill(values, defaultValue);
    }


    /**
     * Constructor
     * @param source    the source array to shallow copy
     * @param parallel  true for parallel version
     */
    private DenseArrayOfObjects(DenseArrayOfObjects<T> source, boolean parallel) {
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
    public final T defaultValue() {
        return defaultValue;
    }


    @Override
    public final Array<T> parallel() {
        return isParallel() ? this : new DenseArrayOfObjects<>(this, true);
    }


    @Override
    public final Array<T> sequential() {
        return isParallel() ? new DenseArrayOfObjects<>(this, false) : this;
    }


    @Override()
    @SuppressWarnings("unchecked")
    public final Array<T> copy() {
        try {
            final DenseArrayOfObjects<T> copy = (DenseArrayOfObjects<T>)super.clone();
            copy.defaultValue = this.defaultValue;
            copy.values = this.values.clone();
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed to copy Array: " + this, ex);
        }
    }


    @Override()
    public final Array<T> copy(int[] indexes) {
        final DenseArrayOfObjects<T> clone = new DenseArrayOfObjects<>(type(), indexes.length, defaultValue);
        for (int i = 0; i < indexes.length; ++i) {
            clone.values[i] = this.values[indexes[i]];
        }
        return clone;
    }


    @Override()
    public final Array<T> copy(int start, int end) {
        final int length = end - start;
        final DenseArrayOfObjects<T> clone = new DenseArrayOfObjects<>(type(), length, defaultValue);
        System.arraycopy(values, start, clone.values, 0, length);
        return clone;
    }


    @Override
    public final Array<T> swap(int i, int j) {
        final Object v1 = values[i];
        final Object v2 = values[j];
        this.values[i] = v2;
        this.values[j] = v1;
        return this;
    }


    @Override
    @SuppressWarnings("unchecked")
    public final int compare(int i, int j) {
        final Comparable c1 = (Comparable)getValue(i);
        final Comparable c2 = (Comparable)getValue(j);
        return (c1 == null ? c2 == null ? 0 : -1 : c2 == null ? 1 : c1.compareTo(c2));
    }


    @Override
    public final Array<T> filter(Predicate<ArrayValue<T>> predicate) {
        final ArrayCursor<T> cursor = cursor();
        final ArrayBuilder<T> builder = ArrayBuilder.of(length(), type());
        for (int i=0; i<values.length; ++i) {
            cursor.moveTo(i);
            final boolean match = predicate.test(cursor);
            if (match) {
                builder.add(cursor.getValue());
            }
        }
        return builder.toArray();
    }


    @Override
    public final Array<T> update(Array<T> from, int[] fromIndexes, int[] toIndexes) {
        if (fromIndexes.length != toIndexes.length) {
            throw new ArrayException("The from index array must have the same length as the to index array");
        } else {
            for (int i=0; i<fromIndexes.length; ++i) {
                final int toIndex = toIndexes[i];
                final int fromIndex = fromIndexes[i];
                final T update = from.getValue(fromIndex);
                this.expand(toIndex);
                this.setValue(toIndex, update);
            }
        }
        return this;
    }


    @Override
    public final Array<T> update(int toIndex, Array<T> from, int fromIndex, int length) {
        for (int i=0; i<length; ++i) {
            final T update = from.getValue(fromIndex + i);
            this.expand(toIndex + i);
            this.setValue(toIndex + i, update);
        }
        return this;
    }


    @Override
    public final Array<T> expand(int newLength) {
        if (newLength > values.length) {
            final int oldCapacity = values.length;
            int newCapacity = oldCapacity + (oldCapacity >> 1);
            if (newCapacity - newLength < 0) newCapacity = newLength;
            final Object[] newValues = new Object[newCapacity];
            System.arraycopy(values, 0, newValues, 0, values.length);
            this.values = newValues;
            Arrays.fill(values, oldCapacity, values.length, defaultValue);
        }
        return this;
    }


    @Override()
    public final Array<T> fill(T value) {
        Arrays.fill(values, value);
        return this;
    }


    @Override
    public Array<T> fill(T value, int start, int end) {
        Arrays.fill(values, start, end, value);
        return this;
    }


    @Override
    public boolean isNull(int index) {
        return values[index] == null;
    }


    @Override
    public final boolean isEqualTo(int index, T value) {
        return value == null ? values[index] == null : value.equals(values[index]);
    }


    @Override
    public final boolean getBoolean(int index) {
        try {
            final Object value = values[index];
            return value != null ? (Boolean)value : false;
        } catch (Exception ex) {
            throw new ArrayException("Array access exception: " + ex.getMessage(), ex);
        }
    }


    @Override
    public final int getInt(int index) {
        try {
            final Object value = values[index];
            return value != null ? ((Number)value).intValue() : 0;
        } catch (Exception ex) {
            throw new ArrayException("Array access exception: " + ex.getMessage(), ex);
        }
    }


    @Override
    public final long getLong(int index) {
        try {
            final Object value = values[index];
            return value != null ? ((Number)value).longValue() : 0L;
        } catch (Exception ex) {
            throw new ArrayException("Array access exception: " + ex.getMessage(), ex);
        }
    }


    @Override
    public final double getDouble(int index) {
        try {
            final Object value = values[index];
            return value != null ? ((Number)value).doubleValue() : Double.NaN;
        } catch (Exception ex) {
            throw new ArrayException("Array access exception: " + ex.getMessage(), ex);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public final T getValue(int index) {
        return (T)values[index];
    }


    @Override
    public final boolean setBoolean(int index, boolean value) {
        final Object oldValue = values[index];
        this.values[index] = value;
        return oldValue instanceof Boolean && (Boolean)oldValue;
    }


    @Override
    public final int setInt(int index, int value) {
        final Object oldValue = values[index];
        this.values[index] = value;
        return oldValue != null ? ((Number)oldValue).intValue() : 0;
    }


    @Override
    public final long setLong(int index, long value) {
        final Object oldValue = values[index];
        this.values[index] = value;
        return oldValue != null ? ((Number)oldValue).longValue() : 0;
    }


    @Override
    public final double setDouble(int index, double value) {
        final Object oldValue = values[index];
        this.values[index] = value;
        return oldValue != null ? ((Number)oldValue).doubleValue() : Double.NaN;
    }


    @Override
    @SuppressWarnings("unchecked")
    public final T setValue(int index, T value) {
        final Object oldValue = values[index];
        this.values[index] = value;
        return (T)oldValue;
    }


    @Override
    public final void read(ObjectInputStream is, int count) throws IOException {
        try {
            for (int i=0; i<count; ++i) {
                this.values[i] = is.readObject();
            }
        } catch (ClassNotFoundException ex) {
            throw new ArrayException("Failed to de-serialized array", ex);
        }
    }


    @Override
    public final void write(ObjectOutputStream os, int[] indexes) throws IOException {
        for (int index : indexes) {
            os.writeObject(values[index]);
        }
    }

    /** Custom serialization */
    private void writeObject(ObjectOutputStream os) throws IOException {
        os.writeInt(values.length);
        for (Object value : values) {
            os.writeObject(value);
        }
    }


    @SuppressWarnings("unchecked")
    /** Custom serialization */
    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
        final int length = is.readInt();
        this.values = new Object[length];
        for (int i=0; i<length; ++i) {
            values[i] = is.readObject();
        }
    }


}
