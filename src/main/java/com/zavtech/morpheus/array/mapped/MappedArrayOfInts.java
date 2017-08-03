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
 * An Array implementation designed to represent a dense array of int values in a memory-mapped file.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class MappedArrayOfInts extends ArrayBase<Integer> {

    private static final long BYTE_COUNT = 4L;

    private File file;
    private int length;
    private int defaultValue;
    private FileChannel channel;
    private IntBuffer buffer;

    /**
     * Constructor
     * @param length        the length of the array
     * @param defaultValue  the default value for array
     * @param file          the memory mapped file reference
     */
    MappedArrayOfInts(int length, Integer defaultValue, File file) {
        super(Integer.class, ArrayStyle.MAPPED, false);
        try {
            this.file = file;
            this.length = length;
            this.defaultValue = defaultValue == null ? 0 : defaultValue;
            this.channel = new RandomAccessFile(file, "rw").getChannel();
            this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, BYTE_COUNT * length).asIntBuffer();
            this.fill(defaultValue);
        } catch (Exception ex) {
            throw new ArrayException("Failed to initialise memory mapped array on file: " + file.getAbsolutePath(), ex);
        }
    }

    /**
     * Constructor
     * @param source    the source array to shallow copy
     * @param parallel  true for parallel version
     */
    private MappedArrayOfInts(MappedArrayOfInts source, boolean parallel) {
        super(source.type(), ArrayStyle.MAPPED, parallel);
        this.file = source.file;
        this.length = source.length;
        this.defaultValue = source.defaultValue;
        this.channel = source.channel;
        this.buffer = source.buffer;
    }

    /**
     * Returns the file handle for this memory mapped array
     * @return      the file handle for memory mapped array
     */
    File getFile() {
        return file;
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
    public final Integer defaultValue() {
        return defaultValue;
    }


    @Override
    public final Array<Integer> parallel() {
        return isParallel() ? this : new MappedArrayOfInts(this, true);
    }


    @Override
    public final Array<Integer> sequential() {
        return isParallel() ? new MappedArrayOfInts(this, false) : this;
    }


    @Override()
    public final Array<Integer> copy() {
        try {
            final File newFile = MappedArrayConstructor.randomFile(true);
            final MappedArrayOfInts copy = new MappedArrayOfInts(length, defaultValue, newFile);
            for (int i=0; i<length; ++i) {
                final int v = getInt(i);
                copy.buffer.put(i, v);
            }
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed to copy Array: " + this, ex);
        }
    }


    @Override()
    public final Array<Integer> copy(int[] indexes) {
        try {
            final File newFile = MappedArrayConstructor.randomFile(true);
            final MappedArrayOfInts copy = new MappedArrayOfInts(indexes.length, defaultValue, newFile);
            for (int i=0; i<indexes.length; ++i) {
                final int value = getInt(indexes[i]);
                if (Integer.compare(value, defaultValue) != 0) {
                    copy.buffer.put(i, value);
                }
            }
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed top copy subset of Array", ex);
        }
    }


    @Override()
    public final Array<Integer> copy(int start, int end) {
        try {
            final int newLength = end - start;
            final File newFile = MappedArrayConstructor.randomFile(true);
            final MappedArrayOfInts copy = new MappedArrayOfInts(newLength, defaultValue, newFile);
            for (int i=0; i<newLength; ++i) {
                final int value = buffer.get(start + i);
                if (Integer.compare(value, defaultValue) != 0) {
                    copy.buffer.put(i, value);
                }
            }
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed top copy subset of Array", ex);
        }
    }


    @Override
    protected final Array<Integer> sort(int start, int end, int multiplier) {
        return doSort(start, end, (i, j) -> {
            final int v1 = getInt(i);
            final int v2 = getInt(j);
            return multiplier * Integer.compare(v1, v2);
        });
    }


    @Override
    public final int compare(int i, int j) {
        final int v1 = getInt(i);
        final int v2 = getInt(j);
        return Integer.compare(v1, v2);
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
        final ArrayCursor<Integer> cursor = cursor();
        final ArrayBuilder<Integer> builder = ArrayBuilder.of(length(), type());
        for (int i=0; i<length(); ++i) {
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
    public final Array<Integer> fill(Integer value, int start, int end) {
        final int fillValue = value == null ? defaultValue : value;
        for (int i=start; i<end; ++i) {
            this.buffer.put(i, fillValue);
        }
        return this;
    }


    @Override
    public final boolean isNull(int index) {
        return false;
    }


    @Override
    public final boolean isEqualTo(int index, Integer value) {
        return value != null && value == buffer.get(index);
    }


    @Override
    public final int getInt(int index) {
        this.checkBounds(index, length);
        return buffer.get(index);
    }


    @Override
    public final long getLong(int index) {
        this.checkBounds(index, length);
        return buffer.get(index);
    }


    @Override
    public final double getDouble(int index) {
        this.checkBounds(index, length);
        return buffer.get(index);
    }


    @Override
    public final Integer getValue(int index) {
        this.checkBounds(index, length);
        return buffer.get(index);
    }


    @Override
    public final int setInt(int index, int value) {
        this.checkBounds(index, length);
        final int oldValue = buffer.get(index);
        this.buffer.put(index, value);
        return oldValue;
    }


    @Override
    public final Integer setValue(int index, Integer value) {
        final Integer oldValue = getValue(index);
        this.buffer.put(index, value != null ? value : defaultValue);
        return oldValue;
    }


    @Override
    public final int binarySearch(int start, int end, Integer value) {
        try {
            int low = start;
            int high = end - 1;
            while (low <= high) {
                final int midIndex = (low + high) >>> 1;
                final int midValue = buffer.get(midIndex);
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
        } catch (Exception ex) {
            throw new ArrayException("Binary search of array failed", ex);
        }
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
        result.setInt(0, buffer.get(0));
        for (int i=1; i<length; ++i) {
            final int prior = result.getInt(i-1);
            final int current = buffer.get(i);
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

    /** Custom serialization */
    private void writeObject(ObjectOutputStream os) throws IOException {
        os.writeInt(length);
        os.writeInt(defaultValue);
        for (int i=0; i<length; ++i) {
            final int value = getInt(i);
            os.writeInt(value);
        }
    }


    @SuppressWarnings("unchecked")
    /** Custom serialization */
    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
        this.length = is.readInt();
        this.defaultValue = is.readInt();
        this.file = MappedArrayConstructor.randomFile(true);
        this.channel = new RandomAccessFile(file, "rw").getChannel();
        this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, BYTE_COUNT * length).asIntBuffer();
        for (int i=0; i<length; ++i) {
            final int value = is.readInt();
            this.setInt(i, value);
        }
    }


}
