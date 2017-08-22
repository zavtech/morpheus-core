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
package com.zavtech.morpheus.array.mapped;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.function.Predicate;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBase;
import com.zavtech.morpheus.array.ArrayBuilder;
import com.zavtech.morpheus.array.ArrayCursor;
import com.zavtech.morpheus.array.ArrayException;
import com.zavtech.morpheus.array.ArrayStyle;
import com.zavtech.morpheus.array.ArrayValue;
import com.zavtech.morpheus.array.coding.IntCoding;
import com.zavtech.morpheus.array.coding.WithIntCoding;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

/**
 * A dense array implementation that maintains a primitive int array of codes that map to Object values exposed through the IntCoding interface.
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
class MappedArrayWithIntCoding<T> extends ArrayBase<T> implements WithIntCoding<T> {

    private static final long serialVersionUID = 1L;

    private static final long BYTE_COUNT = 4L;

    private File file;
    private int length;
    private T defaultValue;
    private int defaultCode;
    private IntCoding<T> coding;
    private FileChannel channel;
    private IntBuffer buffer;

    /**
     * Constructor
     * @param length        the length for this array
     * @param defaultValue  the default value for array
     * @param coding        the coding for this array
     * @param file          the memory mapped file reference
     */
    MappedArrayWithIntCoding(int length, T defaultValue, IntCoding<T> coding, File file) {
        super(coding.getType(), ArrayStyle.MAPPED, false);
        try {
            this.file = file;
            this.length = length;
            this.coding = coding;
            this.defaultValue = defaultValue;
            this.defaultCode = coding.getCode(defaultValue);
            this.channel = new RandomAccessFile(file, "rw").getChannel();
            this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, BYTE_COUNT * length).asIntBuffer();
            this.fill(defaultValue);
        } catch (Exception ex) {
            throw new ArrayException("Failed to initialise memory mapped array on file: " + file.getAbsolutePath(), ex);
        }
    }


    /**
     * Constructor
     * @param source    the source array to copy
     * @param parallel  true for the parallel version
     */
    private MappedArrayWithIntCoding(MappedArrayWithIntCoding<T> source, boolean parallel) {
        super(source.type(), ArrayStyle.DENSE, parallel);
        this.file = source.file;
        this.length = source.length;
        this.coding = source.coding;
        this.defaultValue = source.defaultValue;
        this.defaultCode = source.defaultCode;
        this.channel = source.channel;
        this.buffer = source.buffer;
    }


    @Override
    public final IntCoding<T> getCoding() {
        return coding;
    }


    @Override
    public final int length() {
        return length;
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
        return isParallel() ? this : new MappedArrayWithIntCoding<>(this, true);
    }


    @Override
    public final Array<T> sequential() {
        return isParallel() ? new MappedArrayWithIntCoding<>(this, false) : this;
    }


    @Override()
    @SuppressWarnings("unchecked")
    public final Array<T> copy() {
        try {
            final File newFile = MappedArrayConstructor.randomFile(true);
            final MappedArrayWithIntCoding<T> copy = new MappedArrayWithIntCoding<>(length, defaultValue, coding, newFile);
            for (int i=0; i<length; ++i) {
                final int v = buffer.get(i);
                copy.buffer.put(i, v);
            }
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed to copy Array: " + this, ex);
        }
    }


    @Override()
    public final Array<T> copy(int[] indexes) {
        try {
            final File newFile = MappedArrayConstructor.randomFile(true);
            final MappedArrayWithIntCoding<T> copy = new MappedArrayWithIntCoding<>(indexes.length, defaultValue, coding, newFile);
            for (int i=0; i<indexes.length; ++i) {
                final int value = buffer.get(indexes[i]);
                if (Integer.compare(value, defaultCode) != 0) {
                    copy.buffer.put(i, value);
                }
            }
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed top copy subset of Array", ex);
        }
    }


    @Override()
    public final Array<T> copy(int start, int end) {
        try {
            final int newLength = end - start;
            final File newFile = MappedArrayConstructor.randomFile(true);
            final MappedArrayWithIntCoding<T> copy = new MappedArrayWithIntCoding<>(newLength, defaultValue, coding, newFile);
            for (int i=0; i<newLength; ++i) {
                final int value = buffer.get(start + i);
                if (Integer.compare(value, defaultCode) != 0) {
                    copy.buffer.put(i, value);
                }
            }
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed top copy subset of Array", ex);
        }
    }


    @Override
    protected final Array<T> sort(int start, int end, int multiplier) {
        return doSort(start, end, (i, j) -> {
            final int v1 = buffer.get(i);
            final int v2 = buffer.get(j);
            return multiplier * Integer.compare(v1, v2);
        });
    }


    @Override
    public final int compare(int i, int j) {
        final int v1 = buffer.get(i);
        final int v2 = buffer.get(j);
        return Integer.compare(v1, v2);
    }


    @Override
    public final Array<T> swap(int i, int j) {
        final int v1 = buffer.get(i);
        final int v2 = buffer.get(j);
        this.buffer.put(j, v1);
        this.buffer.put(i, v2);
        return this;
    }


    @Override
    public final Array<T> filter(Predicate<ArrayValue<T>> predicate) {
        final ArrayCursor<T> cursor = cursor();
        final ArrayBuilder<T> builder = ArrayBuilder.of(length(), type());
        for (int i = 0; i< length(); ++i) {
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
                this.setValue(toIndex, update);
            }
        }
        return this;
    }


    @Override
    public final Array<T> update(int toIndex, Array<T> from, int fromIndex, int length) {
        if (from instanceof MappedArrayWithIntCoding) {
            final MappedArrayWithIntCoding other = (MappedArrayWithIntCoding) from;
            for (int i = 0; i < length; ++i) {
                this.buffer.put(toIndex + i, other.buffer.get(fromIndex + i));
            }
        } else {
            for (int i=0; i<length; ++i) {
                final T update = from.getValue(fromIndex + i);
                this.setValue(toIndex + i, update);
            }
        }
        return this;
    }


    @Override
    public final Array<T> expand(int newLength) {
        try {
            if (newLength > length) {
                this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, BYTE_COUNT * newLength).asIntBuffer();
                this.fill(defaultValue, length, newLength);
                this.length = newLength;
            }
            return this;

        } catch (Exception ex) {
            throw new ArrayException("Failed to expand size of memory mapped array at " + file.getAbsolutePath(), ex);
        }
    }


    @Override
    public Array<T> fill(T value, int start, int end) {
        final int code = coding.getCode(value);
        for (int i=start; i<end; ++i) {
            this.buffer.put(i, code);
        }
        return this;
    }


    @Override
    public final boolean isNull(int index) {
        return buffer.get(index) == coding.getCode(null);
    }


    @Override
    public final boolean isEqualTo(int index, T value) {
        if (value == null) {
            return isNull(index);
        } else {
            final int code = coding.getCode(value);
            return code == buffer.get(index);
        }
    }


    @Override
    public int getInt(int index) {
        this.checkBounds(index, length);
        return buffer.get(index);
    }


    @Override
    public final T getValue(int index) {
        this.checkBounds(index, length);
        final int code = buffer.get(index);
        return coding.getValue(code);
    }


    @Override
    public final T setValue(int index, T value) {
        this.checkBounds(index, length);
        final T oldValue = getValue(index);
        this.buffer.put(index, coding.getCode(value));
        return oldValue;
    }


    @Override
    public Array<T> distinct(int limit) {
        final int capacity = limit < Integer.MAX_VALUE ? limit : 100;
        final IntSet set = new IntOpenHashSet(capacity);
        final ArrayBuilder<T> builder = ArrayBuilder.of(capacity, type());
        for (int i=0; i<length(); ++i) {
            final int code = getInt(i);
            if (set.add(code)) {
                final T value = getValue(i);
                builder.add(value);
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
            final int code = is.readInt();
            this.buffer.put(i, code);
        }
    }


    @Override
    public final void write(ObjectOutputStream os, int[] indexes) throws IOException {
        for (int index : indexes) {
            final int code = buffer.get(index);
            os.writeInt(code);
        }
    }

    /** Custom serialization */
    private void writeObject(ObjectOutputStream os) throws IOException {
        os.writeInt(length);
        os.writeInt(defaultCode);
        os.writeObject(defaultValue);
        os.writeObject(coding);
        for (int i=0; i<length; ++i) {
            final int value = buffer.get(i);
            os.writeInt(value);
        }
    }


    @SuppressWarnings("unchecked")
    /** Custom serialization */
    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
        this.file = MappedArrayConstructor.randomFile(true);
        this.length = is.readInt();
        this.defaultCode = is.readInt();
        this.defaultValue = (T)is.readObject();
        this.coding = (IntCoding<T>)is.readObject();
        this.channel = new RandomAccessFile(file, "rw").getChannel();
        this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, BYTE_COUNT * length).asIntBuffer();
        for (int i=0; i<length; ++i) {
            final int value = is.readInt();
            this.buffer.put(i, value);
        }
    }

}
