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
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.util.function.Predicate;

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
class MappedArrayOfLongs extends ArrayBase<Long> {

    private static final long BYTE_COUNT = 8L;

    private File file;
    private int length;
    private long defaultValue;
    private FileChannel channel;
    private LongBuffer buffer;

    /**
     * Constructor
     * @param length        the length of the array
     * @param defaultValue  the default value for array
     * @param file          the memory mapped file reference
     */
    MappedArrayOfLongs(int length, Long defaultValue, File file) {
        super(Long.class, ArrayStyle.MAPPED, false);
        try {
            this.file = file;
            this.length = length;
            this.defaultValue = defaultValue == null ? 0 : defaultValue;
            this.channel = new RandomAccessFile(file, "rw").getChannel();
            this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, BYTE_COUNT * length).asLongBuffer();
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
    private MappedArrayOfLongs(MappedArrayOfLongs source, boolean parallel) {
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
    public final Long defaultValue() {
        return defaultValue;
    }


    @Override
    public final Array<Long> parallel() {
        return isParallel() ? this : new MappedArrayOfLongs(this, true);
    }


    @Override
    public final Array<Long> sequential() {
        return isParallel() ? new MappedArrayOfLongs(this, false) : this;
    }


    @Override()
    public final Array<Long> copy() {
        try {
            final File newFile = MappedArrayConstructor.randomFile(true);
            final MappedArrayOfLongs copy = new MappedArrayOfLongs(length, defaultValue, newFile);
            for (int i=0; i<length; ++i) {
                final long v = getLong(i);
                copy.buffer.put(i, v);
            }
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed to copy Array: " + this, ex);
        }
    }


    @Override()
    public final Array<Long> copy(int[] indexes) {
        try {
            final File newFile = MappedArrayConstructor.randomFile(true);
            final MappedArrayOfLongs copy = new MappedArrayOfLongs(indexes.length, defaultValue, newFile);
            for (int i=0; i<indexes.length; ++i) {
                final long value = getLong(indexes[i]);
                if (Long.compare(value, defaultValue) != 0) {
                    copy.buffer.put(i, value);
                }
            }
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed top copy subset of Array", ex);
        }
    }


    @Override()
    public final Array<Long> copy(int start, int end) {
        try {
            final int newLength = end - start;
            final File newFile = MappedArrayConstructor.randomFile(true);
            final MappedArrayOfLongs copy = new MappedArrayOfLongs(newLength, defaultValue, newFile);
            for (int i=0; i<newLength; ++i) {
                final long value = buffer.get(start + i);
                if (Long.compare(value, defaultValue) != 0) {
                    copy.buffer.put(i, value);
                }
            }
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed top copy subset of Array", ex);
        }
    }


    @Override
    protected final Array<Long> sort(int start, int end, int multiplier) {
        return doSort(start, end, (i, j) -> {
            final long v1 = getLong(i);
            final long v2 = getLong(j);
            return multiplier * Long.compare(v1, v2);
        });
    }


    @Override
    public final int compare(int i, int j) {
        final long v1 = getLong(i);
        final long v2 = getLong(j);
        return Long.compare(v1, v2);
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
    public final Array<Long> filter(Predicate<ArrayValue<Long>> predicate) {
        final ArrayCursor<Long> cursor = cursor();
        final ArrayBuilder<Long> builder = ArrayBuilder.of(length(), type());
        for (int i=0; i<length(); ++i) {
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
                this.expand(toIndex);
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
        try {
            if (newLength > length) {
                final int oldLength = length;
                int newCapacity = oldLength + (oldLength >> 1);
                if (newCapacity - newLength < 0) newCapacity = newLength;
                this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, BYTE_COUNT * newCapacity).asLongBuffer();
                for (int i=oldLength; i<newLength; ++i) {
                    this.setLong(i, defaultValue);
                }
            }
            return this;
        } catch (Exception ex) {
            throw new ArrayException("Failed to expand size of memory mapped array at " + file.getAbsolutePath(), ex);
        }
    }


    @Override()
    public final Array<Long> fill(Long value) {
        final long fillValue = value == null ? defaultValue : value;
        for (int i=0; i<length; ++i) {
            this.buffer.put(i, fillValue);
        }
        return this;
    }


    @Override
    public final Array<Long> fill(Long value, int start, int end) {
        final long fillValue = value == null ? defaultValue : value;
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
    public final boolean isEqualTo(int index, Long value) {
        return value != null && value == buffer.get(index);
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
    public final Long getValue(int index) {
        this.checkBounds(index, length);
        return buffer.get(index);
    }


    @Override
    public final long setLong(int index, long value) {
        this.checkBounds(index, length);
        final long oldValue = buffer.get(index);
        this.buffer.put(index, value);
        return oldValue;
    }


    @Override
    public final Long setValue(int index, Long value) {
        this.checkBounds(index, length);
        final Long oldValue = getValue(index);
        this.buffer.put(index, value != null ? value : defaultValue);
        return oldValue;
    }


    @Override
    public int binarySearch(int start, int end, Long value) {
        try {
            int low = start;
            int high = end - 1;
            while (low <= high) {
                final int midIndex = (low + high) >>> 1;
                final long midValue = buffer.get(midIndex);
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
        } catch (Exception ex) {
            throw new ArrayException("Binary search of array failed", ex);
        }
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

    /** Custom serialization */
    private void writeObject(ObjectOutputStream os) throws IOException {
        os.writeInt(length);
        os.writeLong(defaultValue);
        for (int i=0; i<length; ++i) {
            final long value = getLong(i);
            os.writeLong(value);
        }
    }


    @SuppressWarnings("unchecked")
    /** Custom serialization */
    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
        this.file = MappedArrayConstructor.randomFile(true);
        this.length = is.readInt();
        this.defaultValue = is.readLong();
        this.channel = new RandomAccessFile(file, "rw").getChannel();
        this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, BYTE_COUNT * length).asLongBuffer();
        for (int i=0; i<length; ++i) {
            final long value = is.readLong();
            this.setLong(i, value);
        }
    }


}
