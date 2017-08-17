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
import com.zavtech.morpheus.array.ArrayCursor;
import com.zavtech.morpheus.array.ArrayException;
import com.zavtech.morpheus.array.ArrayStyle;
import com.zavtech.morpheus.array.ArrayValue;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * An Array implementation designed to hold a sparse array of Object values
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class SparseArrayOfObjects<T> extends ArrayBase<T> {

    private static final long serialVersionUID = 1L;

    private int length;
    private T defaultValue;
    private Int2ObjectMap<Object> values;

    /**
     * Constructor
     * @param type      the type for this array
     * @param length    the length for this array
     * @param defaultValue  the default value
     */
    SparseArrayOfObjects(Class<T> type, int length, T defaultValue) {
        super(type, ArrayStyle.SPARSE, false);
        this.length = length;
        this.defaultValue = defaultValue;
        this.values = new Int2ObjectOpenHashMap<>((int)Math.max(length * 0.5, 10d));
    }

    /**
     * Constructor
     * @param source    the source array to shallow copy
     * @param parallel  true for the parallel version
     */
    private SparseArrayOfObjects(SparseArrayOfObjects<T> source, boolean parallel) {
        super(source.type(), ArrayStyle.SPARSE, parallel);
        this.length = source.length;
        this.defaultValue = source.defaultValue;
        this.values = source.values;
        this.values.defaultReturnValue(this.defaultValue);
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
    public final T defaultValue() {
        return defaultValue;
    }

    @Override
    public final Array<T> parallel() {
        return isParallel() ? this : new SparseArrayOfObjects<>(this, true);
    }


    @Override
    public final Array<T> sequential() {
        return isParallel() ? new SparseArrayOfObjects<>(this, false) : this;
    }


    @Override()
    @SuppressWarnings("unchecked")
    public final Array<T> copy() {
        try {
            final SparseArrayOfObjects<T> copy = (SparseArrayOfObjects<T>)super.clone();
            copy.values = new Int2ObjectOpenHashMap<>(values);
            copy.defaultValue = this.defaultValue;
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed to copy Array: " + this, ex);
        }
    }


    @Override()
    public final Array<T> copy(int[] indexes) {
        final SparseArrayOfObjects<T> clone = new SparseArrayOfObjects<>(type(), indexes.length, defaultValue);
        for (int i = 0; i < indexes.length; ++i) {
            final T value = getValue(indexes[i]);
            clone.setValue(i, value);
        }
        return clone;
    }


    @Override()
    public final Array<T> copy(int start, int end) {
        final int length = end - start;
        final SparseArrayOfObjects<T> clone = new SparseArrayOfObjects<>(type(), length, defaultValue);
        for (int i=0; i<length; ++i) {
            final T value = getValue(start+i);
            //todo: Fix object equality check
            if (value != defaultValue) {
                clone.setValue(i, value);
            }
        }
        return clone;
    }


    @Override
    @SuppressWarnings("unchecked")
    public final int compare(int i, int j) {
        final Comparable c1 = (Comparable)getValue(i);
        final Comparable c2 = (Comparable)getValue(j);
        return (c1 == null ? c2 == null ? 0 : -1 : c2 == null ? 1 : c1.compareTo(c2));
    }


    @Override
    public final Array<T> swap(int i, int j) {
        final T v1 = getValue(i);
        final T v2 = getValue(j);
        this.setValue(i, v2);
        this.setValue(j, v1);
        return this;
    }


    @Override
    public final Array<T> filter(Predicate<ArrayValue<T>> predicate) {
        int count = 0;
        final int length = this.length();
        final ArrayCursor<T> cursor = cursor();
        final Array<T> matches = Array.of(type(), length, loadFactor());
        for (int i=0; i<length; ++i) {
            cursor.moveTo(i);
            final boolean match = predicate.test(cursor);
            if (match) matches.setValue(count++, cursor.getValue());
        }
        return count == length ? matches : matches.copy(0, count);
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
                this.setValue(toIndex, update);
            }
        }
        return this;
    }


    @Override
    public final Array<T> update(int toIndex, Array<T> from, int fromIndex, int length) {
        for (int i=0; i<length; ++i) {
            final T update = from.getValue(fromIndex + i);
            this.setValue(toIndex + i, update);
        }
        return this;
    }


    @Override
    public final Array<T> expand(int newLength) {
        this.length = newLength > length ? newLength : length;
        return this;
    }


    @Override
    public Array<T> fill(T value, int start, int end) {
        if (value == defaultValue || (value != null && value.equals(defaultValue()))) {
            this.values.clear();
        } else {
            for (int i=start; i<end; ++i) {
                this.values.put(i, value);
            }
        }
        return this;
    }


    @Override
    public final boolean isNull(int index) {
        return values.get(index) == null;
    }


    @Override
    public final boolean isEqualTo(int index, T value) {
        return value == null ? isNull(index) : value.equals(values.get(index));
    }


    @Override
    public final boolean getBoolean(int index) {
        this.checkBounds(index, length);
        try {
            if (values.containsKey(index)) {
                final Object value = values.get(index);
                return value != null ? (Boolean)value : false;
            } else {
                return defaultValue != null ? (Boolean)defaultValue : false;
            }
        } catch (Exception ex) {
            throw new ArrayException("Array access exception: " + ex.getMessage(), ex);
        }
    }


    @Override
    public final int getInt(int index) {
        this.checkBounds(index, length);
        try {
            if (values.containsKey(index)) {
                final Object value = values.get(index);
                return value != null ? ((Number)value).intValue() : 0;
            } else {
                return defaultValue != null ? ((Number)defaultValue).intValue() : 0;
            }
        } catch (Exception ex) {
            throw new ArrayException("Array access exception: " + ex.getMessage(), ex);
        }
    }


    @Override
    public final long getLong(int index) {
        this.checkBounds(index, length);
        try {
            if (values.containsKey(index)) {
                final Object value = values.get(index);
                return value != null ? ((Number)value).longValue() : 0L;
            } else {
                return defaultValue != null ? ((Number)defaultValue).longValue() : 0L;
            }
        } catch (Exception ex) {
            throw new ArrayException(ex.getMessage(), ex);
        }
    }


    @Override
    public final double getDouble(int index) {
        this.checkBounds(index, length);
        try {
            if (values.containsKey(index)) {
                final Object value = values.get(index);
                return value != null ? ((Number)value).doubleValue() : Double.NaN;
            } else {
                return defaultValue != null ? ((Number)defaultValue).doubleValue() : Double.NaN;
            }
        } catch (Exception ex) {
            throw new ArrayException(ex.getMessage(), ex);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public final T getValue(int index) {
        this.checkBounds(index, length);
        try {
            if (values.containsKey(index)) {
                return (T)values.get(index);
            } else {
                return defaultValue;
            }
        } catch (Exception ex) {
            throw new ArrayException("Array access exception: " + ex.getMessage(), ex);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public final boolean setBoolean(int index, boolean value) {
        this.checkBounds(index, length);
        final boolean oldValue = getBoolean(index);
        this.setValue(index, value ? (T)Boolean.TRUE : (T)Boolean.FALSE);
        return oldValue;
    }


    @Override
    @SuppressWarnings("unchecked")
    public final int setInt(int index, int value) {
        this.checkBounds(index, length);
        final int oldValue = getInt(index);
        this.setValue(index, (T)new Integer(value));
        return oldValue;
    }


    @Override
    @SuppressWarnings("unchecked")
    public final long setLong(int index, long value) {
        this.checkBounds(index, length);
        final long oldValue = getLong(index);
        this.setValue(index, (T)new Long(value));
        return oldValue;
    }


    @Override
    @SuppressWarnings("unchecked")
    public final double setDouble(int index, double value) {
        this.checkBounds(index, length);
        final double oldValue = getDouble(index);
        this.setValue(index, (T)new Double(value));
        return oldValue;
    }


    @Override
    public final T setValue(int index, T value) {
        this.checkBounds(index, length);
        final T oldValue = getValue(index);
        if (value == defaultValue || value != null && value.equals(defaultValue)) {
            this.values.remove(index);
        } else if (value == null) {
            this.values.put(index, null);
        } else {
            final Class<?> expectedType = type();
            final Class<?> actualType = value.getClass();
            if (actualType == expectedType || expectedType.isAssignableFrom(value.getClass())) {
                this.values.put(index, value);
            } else {
                final String details = "Expected type: " + expectedType.getSimpleName() + ", actual type: " + actualType.getSimpleName();
                throw new ArrayException("Value is not compatible with array type: " + details);
            }
        }
        return oldValue;
    }


    @Override
    @SuppressWarnings("unchecked")
    public final void read(ObjectInputStream is, int count) throws IOException {
        try {
            for (int i=0; i<count; ++i) {
                final T value = (T)is.readObject();
                this.setValue(i, value);
            }
        } catch (ClassNotFoundException ex) {
            throw new ArrayException("Failed to de-serialize array", ex);
        }
    }


    @Override
    public final void write(ObjectOutputStream os, int[] indexes) throws IOException {
        for (int index : indexes) {
            final T value = getValue(index);
            os.writeObject(value);
        }
    }

}
