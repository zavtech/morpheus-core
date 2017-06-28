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
import com.zavtech.morpheus.array.ArrayException;
import com.zavtech.morpheus.array.ArrayStyle;
import com.zavtech.morpheus.array.coding.LongCoding;
import com.zavtech.morpheus.array.coding.WithLongCoding;

/**
 * A dense array implementation that maintains a primitive long array of codes that apply to Object values exposed through the LongCoding interface.
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
class DenseArrayWithLongCoding<T> extends ArrayBase<T> implements WithLongCoding<T> {

    private static final long serialVersionUID = 1L;

    private long[] codes;
    private T defaultValue;
    private long defaultCode;
    private LongCoding<T> coding;

    /**
     * Constructor
     * @param length        the length for this array
     * @param defaultValue  the default value for array
     * @param coding        the coding for this array
     */
    DenseArrayWithLongCoding(int length, T defaultValue, LongCoding<T> coding) {
        super(coding.getType(), ArrayStyle.DENSE, false);
        this.coding = coding;
        this.codes = new long[length];
        this.defaultValue = defaultValue;
        this.defaultCode = coding.getCode(defaultValue);
        Arrays.fill(codes, defaultCode);
    }

    /**
     * Constructor
     * @param source    the source array to copy
     * @param parallel  true for the parallel version
     */
    private DenseArrayWithLongCoding(DenseArrayWithLongCoding<T> source, boolean parallel) {
        super(source.type(), ArrayStyle.DENSE, parallel);
        this.coding = source.coding;
        this.codes = source.codes;
        this.defaultValue = source.defaultValue;
        this.defaultCode = source.defaultCode;
    }


    @Override
    public LongCoding<T> getCoding() {
        return coding;
    }


    @Override
    public final int length() {
        return codes.length;
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
        return isParallel() ? this : new DenseArrayWithLongCoding<>(this, true);
    }


    @Override
    public final Array<T> sequential() {
        return isParallel() ? new DenseArrayWithLongCoding<>(this, false) : this;
    }


    @Override()
    @SuppressWarnings("unchecked")
    public final Array<T> copy() {
        try {
            final DenseArrayWithLongCoding<T> copy = (DenseArrayWithLongCoding<T>)super.clone();
            copy.defaultValue = this.defaultValue;
            copy.defaultCode = this.defaultCode;
            copy.coding = this.coding;
            copy.codes = this.codes.clone();
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed to copy Array: " + this, ex);
        }
    }


    @Override()
    public final Array<T> copy(int[] indexes) {
        final DenseArrayWithLongCoding<T> clone = new DenseArrayWithLongCoding<>(indexes.length, defaultValue, coding);
        for (int i = 0; i < indexes.length; ++i) {
            clone.codes[i] = this.codes[indexes[i]];
        }
        return clone;
    }


    @Override()
    public final Array<T> copy(int start, int end) {
        final int length = end - start;
        final DenseArrayWithLongCoding<T> clone = new DenseArrayWithLongCoding<>(length, defaultValue, coding);
        System.arraycopy(codes, start, clone.codes, 0, length);
        return clone;
    }


    @Override
    protected final Array<T> sort(int start, int end, int multiplier) {
        return doSort(start, end, (i, j) -> multiplier * Long.compare(codes[i], codes[j]));
    }


    @Override
    public final int compare(int i, int j) {
        return Long.compare(codes[i], codes[j]);
    }


    @Override
    public final Array<T> swap(int i, int j) {
        final long tmp = codes[i];
        this.codes[i] = codes[j];
        this.codes[j] = tmp;
        return this;
    }


    @Override
    public final Array<T> filter(Predicate<T> predicate) {
        final ArrayBuilder<T> builder = ArrayBuilder.of(length(), type());
        for (int i = 0; i< length(); ++i) {
            final T value = getValue(i);
            final boolean match = predicate.test(value);
            if (match) {
                builder.add(value);
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
        if (from instanceof DenseArrayWithLongCoding) {
            final DenseArrayWithLongCoding other = (DenseArrayWithLongCoding) from;
            for (int i = 0; i < length; ++i) {
                this.expand(toIndex + i);
                this.codes[toIndex + i] = other.codes[fromIndex + i];
            }
        } else if (from instanceof DenseArrayOfLongs) {
            for (int i = 0; i < length; ++i) {
                this.expand(toIndex + i);
                this.codes[toIndex + i] = from.getLong(fromIndex + i);
            }
        } else {
            for (int i=0; i<length; ++i) {
                final T update = from.getValue(fromIndex + i);
                this.expand(toIndex + i);
                this.setValue(toIndex + i, update);
            }
        }
        return this;
    }


    @Override
    public final Array<T> expand(int newLength) {
        if (newLength > codes.length) {
            final int oldCapacity = codes.length;
            int newCapacity = oldCapacity + (oldCapacity >> 1);
            if (newCapacity - newLength < 0) newCapacity = newLength;
            final long[] newCodes = new long[newCapacity];
            System.arraycopy(codes, 0, newCodes, 0, codes.length);
            this.codes = newCodes;
            Arrays.fill(codes, oldCapacity, codes.length, defaultCode);
        }
        return this;
    }


    @Override()
    public final Array<T> fill(T value) {
        final long code = coding.getCode(value);
        Arrays.fill(codes, code);
        return this;
    }


    @Override
    public Array<T> fill(T value, int start, int end) {
        final long code = coding.getCode(value);
        Arrays.fill(codes, start, end, code);
        return this;
    }


    @Override
    public final boolean isNull(int index) {
        return codes[index] == coding.getCode(null);
    }


    @Override
    public final boolean isEqualTo(int index, T value) {
        if (value == null) {
            return isNull(index);
        } else {
            final long code = coding.getCode(value);
            return code == codes[index];
        }
    }


    @Override
    public long getLong(int index) {
        return codes[index];
    }


    @Override
    public final T getValue(int index) {
        return coding.getValue(codes[index]);
    }


    @Override
    public long setLong(int index, long value) {
        final long oldValue = codes[index];
        this.codes[index] = value;
        return oldValue;
    }


    @Override
    public final T setValue(int index, T value) {
        final T oldValue = getValue(index);
        this.codes[index] = coding.getCode(value);
        return oldValue;
    }


    @Override
    public final void read(ObjectInputStream is, int count) throws IOException {
        for (int i=0; i<count; ++i) {
            this.codes[i] = is.readLong();
        }
    }


    @Override
    public final void write(ObjectOutputStream os, int[] indexes) throws IOException {
        for (int index : indexes) {
            os.writeLong(codes[index]);
        }
    }

    /** Custom serialization */
    private void writeObject(ObjectOutputStream os) throws IOException {
        os.writeObject(coding);
        os.writeInt(codes.length);
        for (long value : codes) {
            os.writeLong(value);
        }
    }


    @SuppressWarnings("unchecked")
    /** Custom serialization */
    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
        this.coding = (LongCoding<T>)is.readObject();
        final int length = is.readInt();
        this.codes = new long[length];
        for (int i=0; i<length; ++i) {
            codes[i] = is.readLong();
        }
    }
}
