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
import java.nio.ShortBuffer;
import java.nio.channels.FileChannel;
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
 * An Array implementation designed to hold a mapped array of boolean values
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class MappedArrayOfBooleans extends ArrayBase<Boolean> {

    private static final long serialVersionUID = 1L;

    private static final long BYTE_COUNT = 2L;

    private File file;
    private int length;
    private boolean defaultValue;
    private FileChannel channel;
    private ShortBuffer buffer;

    /**
     * Constructor
     * @param length        the length for this array
     * @param defaultValue  the default value for array
     * @param file          the memory mapped file reference
     */
    MappedArrayOfBooleans(int length, Boolean defaultValue , File file) {
        super(Boolean.class, ArrayStyle.MAPPED, false);
        try {
            this.file = file;
            this.length = length;
            this.defaultValue = defaultValue == null ? false : defaultValue;
            this.channel = new RandomAccessFile(file, "rw").getChannel();
            this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, BYTE_COUNT * length).asShortBuffer();
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
    private MappedArrayOfBooleans(MappedArrayOfBooleans source, boolean parallel) {
        super(source.type(), ArrayStyle.MAPPED, parallel);
        this.file = source.file;
        this.length = source.length;
        this.defaultValue = source.defaultValue;
        this.channel = source.channel;
        this.buffer = source.buffer;
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
    public final Boolean defaultValue() {
        return defaultValue;
    }


    @Override
    public final Array<Boolean> parallel() {
        return isParallel() ? this : new MappedArrayOfBooleans(this, true);
    }


    @Override
    public final Array<Boolean> sequential() {
        return isParallel() ? new MappedArrayOfBooleans(this, false) : this;
    }


    @Override()
    public final Array<Boolean> copy() {
        try {
            final File newFile = MappedArrayConstructor.randomFile(true);
            final short defaultShort = defaultValue ? (short)1 : (short)0;
            final MappedArrayOfBooleans copy = new MappedArrayOfBooleans(length, defaultValue, newFile);
            for (int i=0; i<length; ++i) {
                final short value = buffer.get(i);
                if (value != defaultShort) {
                    copy.buffer.put(i, value);
                }
            }
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed to copy Array: " + this, ex);
        }
    }


    @Override()
    public final Array<Boolean> copy(int[] indexes) {
        try {
            final File newFile = MappedArrayConstructor.randomFile(true);
            final short defaultShort = defaultValue ? (short)1 : (short)0;
            final MappedArrayOfBooleans copy = new MappedArrayOfBooleans(indexes.length, defaultValue, newFile);
            for (int i=0; i<indexes.length; ++i) {
                final short value = buffer.get(indexes[i]);
                if (value != defaultShort) {
                    copy.buffer.put(i, value);
                }
            }
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed top copy subset of Array", ex);
        }
    }


    @Override()
    public final Array<Boolean> copy(int start, int end) {
        try {
            final int newLength = end - start;
            final File newFile = MappedArrayConstructor.randomFile(true);
            final short defaultShort = defaultValue ? (short)1 : (short)0;
            final MappedArrayOfBooleans copy = new MappedArrayOfBooleans(newLength, defaultValue, newFile);
            for (int i=0; i<newLength; ++i) {
                final short value = buffer.get(start + i);
                if (value != defaultShort) {
                    copy.buffer.put(i, value);
                }
            }
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed top copy subset of Array", ex);
        }
    }


    @Override
    protected final Array<Boolean> sort(int start, int end, int multiplier) {
        return doSort(start, end, (i, j) -> {
            final boolean v1 = buffer.get(i) == 1;
            final boolean v2 = buffer.get(j) == 1;
            return multiplier * Boolean.compare(v1, v2);
        });
    }


    @Override
    public final int compare(int i, int j) {
        final boolean v1 = buffer.get(i) == 1;
        final boolean v2 = buffer.get(j) == 1;
        return Boolean.compare(v1, v2);
    }


    @Override
    public final Array<Boolean> swap(int i, int j) {
        final short v1 = buffer.get(i);
        final short v2 = buffer.get(j);
        this.buffer.put(j, v1);
        this.buffer.put(i, v2);
        return this;
    }


    @Override
    public final Array<Boolean> filter(Predicate<ArrayValue<Boolean>> predicate) {
        final ArrayCursor<Boolean> cursor = cursor();
        final ArrayBuilder<Boolean> builder = ArrayBuilder.of(length(), type());
        for (int i=0; i<length(); ++i) {
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
        try {
            if (newLength > length) {
                this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, BYTE_COUNT * newLength).asShortBuffer();
                this.fill(defaultValue, length, newLength);
                this.length = newLength;
            }
            return this;
        } catch (Exception ex) {
            throw new ArrayException("Failed to expand size of memory mapped array at " + file.getAbsolutePath(), ex);
        }
    }


    @Override
    public Array<Boolean> fill(Boolean value, int start, int end) {
        final boolean fillValue = value == null ? defaultValue : value;
        final short fillShort = fillValue ? (short)1 : (short)0;
        for (int i=start; i<end; ++i) {
            this.buffer.put(i, fillShort);
        }
        return this;
    }


    @Override
    public boolean isNull(int index) {
        return false;
    }


    @Override
    public final boolean isEqualTo(int index, Boolean value) {
        return value != null && getBoolean(index) == value;
    }


    @Override
    public final boolean getBoolean(int index) {
        this.checkBounds(index, length);
        return this.buffer.get(index) == 1;
    }


    @Override
    public final Boolean getValue(int index) {
        this.checkBounds(index, length);
        return this.buffer.get(index) == 1 ? Boolean.TRUE : Boolean.FALSE;
    }


    @Override
    public final boolean setBoolean(int index, boolean value) {
        this.checkBounds(index, length);
        final boolean oldValue = getBoolean(index);
        this.buffer.put(index, value ? (short)1 : (short)0);
        return oldValue;
    }


    @Override
    public final Boolean setValue(int index, Boolean value) {
        final boolean oldValue = getBoolean(index);
        if (value == null) {
            this.buffer.put(index, defaultValue ? (short)1 : (short)0);
            return oldValue;
        } else {
            this.buffer.put(index, value ? (short)1 : (short)0);
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
        final int capacity = limit < it.unimi.dsi.fastutil.Arrays.MAX_ARRAY_SIZE ? limit : 1000;
        final ShortSet set = new ShortOpenHashSet(capacity);
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
            final boolean value = is.readBoolean();
            this.setBoolean(i, value);
        }
    }

    @Override
    public final void write(ObjectOutputStream os, int[] indexes) throws IOException {
        for (int index : indexes) {
            final boolean value = getBoolean(index);
            os.writeBoolean(value);
        }
    }

    /** Custom serialization */
    private void writeObject(ObjectOutputStream os) throws IOException {
        os.writeInt(length);
        os.writeBoolean(defaultValue);
        for (int i=0; i<length; ++i) {
            final boolean value = getBoolean(i);
            os.writeBoolean(value);
        }
    }


    @SuppressWarnings("unchecked")
    /** Custom serialization */
    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
        this.file = MappedArrayConstructor.randomFile(true);
        this.length = is.readInt();
        this.defaultValue = is.readBoolean();
        this.channel = new RandomAccessFile(file, "rw").getChannel();
        this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, BYTE_COUNT * length).asShortBuffer();
        for (int i=0; i<length; ++i) {
            final boolean value = is.readBoolean();
            this.setBoolean(i, value);
        }
    }

}
