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
import java.nio.DoubleBuffer;
import java.nio.channels.FileChannel;
import java.util.function.Predicate;

import gnu.trove.set.TDoubleSet;
import gnu.trove.set.hash.TDoubleHashSet;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBase;
import com.zavtech.morpheus.array.ArrayBuilder;
import com.zavtech.morpheus.array.ArrayCursor;
import com.zavtech.morpheus.array.ArrayException;
import com.zavtech.morpheus.array.ArrayStyle;
import com.zavtech.morpheus.array.ArrayValue;

/**
 * An Array implementation designed to represent a dense array of double values in a memory-mapped file.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class MappedArrayOfDoubles extends ArrayBase<Double> {

    private static final long BYTE_COUNT = 8L;

    private File file;
    private int length;
    private double defaultValue;
    private FileChannel channel;
    private DoubleBuffer buffer;

    /**
     * Constructor
     * @param length        the length of the array
     * @param defaultValue  the default value for array
     * @param file          the memory mapped file reference
     */
    MappedArrayOfDoubles(int length, Double defaultValue, File file) {
        super(Double.class, ArrayStyle.MAPPED, false);
        try {
            this.file = file;
            this.length = length;
            this.defaultValue = defaultValue == null ? Double.NaN : defaultValue;
            this.channel = new RandomAccessFile(file, "rw").getChannel();
            this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, BYTE_COUNT * length).asDoubleBuffer();
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
    private MappedArrayOfDoubles(MappedArrayOfDoubles source, boolean parallel) {
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
    public final Double defaultValue() {
        return defaultValue;
    }


    @Override
    public final Array<Double> parallel() {
        return isParallel() ? this : new MappedArrayOfDoubles(this, true);
    }


    @Override
    public final Array<Double> sequential() {
        return isParallel() ? new MappedArrayOfDoubles(this, false) : this;
    }


    @Override()
    public final Array<Double> copy() {
        try {
            final File newFile = MappedArrayConstructor.randomFile(true);
            final MappedArrayOfDoubles copy = new MappedArrayOfDoubles(length, defaultValue, newFile);
            for (int i=0; i<length; ++i) {
                final double v = buffer.get(i);
                copy.buffer.put(i, v);
            }
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed to copy Array: " + this, ex);
        }
    }


    @Override()
    public final Array<Double> copy(int[] indexes) {
        try {
            final File newFile = MappedArrayConstructor.randomFile(true);
            final MappedArrayOfDoubles copy = new MappedArrayOfDoubles(indexes.length, defaultValue, newFile);
            for (int i=0; i<indexes.length; ++i) {
                final double value = getDouble(indexes[i]);
                if (Double.compare(value, defaultValue) != 0) {
                    copy.buffer.put(i, value);
                }
            }
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed top copy subset of Array", ex);
        }
    }


    @Override()
    public final Array<Double> copy(int start, int end) {
        try {
            final int newLength = end - start;
            final File newFile = MappedArrayConstructor.randomFile(true);
            final MappedArrayOfDoubles copy = new MappedArrayOfDoubles(newLength, defaultValue, newFile);
            for (int i=0; i<newLength; ++i) {
                final double value = buffer.get(start + i);
                if (Double.compare(value, defaultValue) != 0) {
                    copy.buffer.put(i, value);
                }
            }
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed top copy subset of Array", ex);
        }
    }


    @Override
    protected final Array<Double> sort(int start, int end, int multiplier) {
        return doSort(start, end, (i, j) -> {
            final double v1 = getDouble(i);
            final double v2 = getDouble(j);
            return multiplier * Double.compare(v1, v2);
        });
    }


    @Override
    public final int compare(int i, int j) {
        final double v1 = getDouble(i);
        final double v2 = getDouble(j);
        return Double.compare(v1, v2);
    }


    @Override
    public final Array<Double> swap(int i, int j) {
        final double v1 = getDouble(i);
        final double v2 = getDouble(j);
        this.setDouble(i, v2);
        this.setDouble(j, v1);
        return this;
    }


    @Override
    public final Array<Double> filter(Predicate<ArrayValue<Double>> predicate) {
        final ArrayCursor<Double> cursor = cursor();
        final ArrayBuilder<Double> builder = ArrayBuilder.of(length(), type());
        for (int i=0; i<length(); ++i) {
            cursor.moveTo(i);
            final boolean match = predicate.test(cursor);
            if (match) {
                builder.addDouble(cursor.getDouble());
            }
        }
        return builder.toArray();
    }


    @Override
    public final Array<Double> update(Array<Double> from, int[] fromIndexes, int[] toIndexes) {
        if (fromIndexes.length != toIndexes.length) {
            throw new ArrayException("The from index array must have the same length as the to index array");
        } else {
            for (int i=0; i<fromIndexes.length; ++i) {
                final int toIndex = toIndexes[i];
                final int fromIndex = fromIndexes[i];
                final double update = from.getDouble(fromIndex);
                this.setDouble(toIndex, update);
            }
        }
        return this;
    }


    @Override
    public final Array<Double> update(int toIndex, Array<Double> from, int fromIndex, int length) {
        for (int i=0; i<length; ++i) {
            final double update = from.getDouble(fromIndex + i);
            this.setDouble(toIndex + i, update);
        }
        return this;
    }


    @Override
    public final Array<Double> expand(int newLength) {
        try {
            if (newLength > length) {
                this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, BYTE_COUNT * newLength).asDoubleBuffer();
                this.fill(defaultValue, length, newLength);
                this.length = newLength;
            }
            return this;
        } catch (Exception ex) {
            throw new ArrayException("Failed to expand size of memory mapped array at " + file.getAbsolutePath(), ex);
        }
    }


    @Override
    public final Array<Double> fill(Double value, int start, int end) {
        final double fillValue = value == null ? defaultValue : value;
        for (int i=start; i<end; ++i) {
            this.buffer.put(i, fillValue);
        }
        return this;
    }


    @Override
    public final boolean isNull(int index) {
        return Double.isNaN(getDouble(index));
    }


    @Override
    public final boolean isEqualTo(int index, Double value) {
        return value == null || Double.isNaN(value) ? Double.isNaN(getDouble(index)) : getDouble(index) == value;
    }


    @Override
    public final double getDouble(int index) {
        this.checkBounds(index, length);
        return buffer.get(index);
    }


    @Override
    public final Double getValue(int index) {
        this.checkBounds(index, length);
        return buffer.get(index);
    }


    @Override
    public final double setDouble(int index, double value) {
        this.checkBounds(index, length);
        final double oldValue = buffer.get(index);
        this.buffer.put(index, value);
        return oldValue;
    }


    @Override
    public final Double setValue(int index, Double value) {
        this.checkBounds(index, length);
        final Double oldValue = getValue(index);
        this.buffer.put(index, value != null ? value : defaultValue);
        return oldValue;
    }


    @Override
    public final int binarySearch(int start, int end, Double value) {
        try {
            int low = start;
            int high = end - 1;
            while (low <= high) {
                final int midIndex = (low + high) >>> 1;
                final double midValue = buffer.get(midIndex);
                final int result = Double.compare(midValue, value);
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
    public final Array<Double> distinct(int limit) {
        final int capacity = limit < Integer.MAX_VALUE ? limit : 100;
        final TDoubleSet set = new TDoubleHashSet(capacity);
        final ArrayBuilder<Double> builder = ArrayBuilder.of(capacity, Double.class);
        for (int i=0; i<length(); ++i) {
            final double value = getDouble(i);
            if (set.add(value)) {
                builder.addDouble(value);
                if (set.size() >= limit) {
                    break;
                }
            }
        }
        return builder.toArray();
    }


    @Override
    public final Array<Double> cumSum() {
        final int length = length();
        final Array<Double> result = Array.of(Double.class, length);
        result.setDouble(0, getDouble(0));
        for (int i=1; i<length; ++i) {
            final double prior = result.getDouble(i-1);
            final double current = buffer.get(i);
            if (Double.isNaN(prior)) {
                result.setDouble(i, current);
            } else if (Double.isNaN(current)) {
                result.setDouble(i, prior);
            } else {
                result.setDouble(i, prior + current);
            }
        }
        return result;
    }


    @Override
    public final void read(ObjectInputStream is, int count) throws IOException {
        for (int i=0; i<count; ++i) {
            final double value = is.readDouble();
            this.setDouble(i, value);
        }
    }


    @Override
    public final void write(ObjectOutputStream os, int[] indexes) throws IOException {
        for (int index : indexes) {
            final double value = getDouble(index);
            os.writeDouble(value);
        }
    }

    /** Custom serialization */
    private void writeObject(ObjectOutputStream os) throws IOException {
        os.writeInt(length);
        os.writeDouble(defaultValue);
        for (int i=0; i<length; ++i) {
            final double value = getDouble(i);
            os.writeDouble(value);
        }
    }


    @SuppressWarnings("unchecked")
    /** Custom serialization */
    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
        this.file = MappedArrayConstructor.randomFile(true);
        this.length = is.readInt();
        this.defaultValue = is.readDouble();
        this.channel = new RandomAccessFile(file, "rw").getChannel();
        this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, BYTE_COUNT * length).asDoubleBuffer();
        for (int i=0; i<length; ++i) {
            final double value = is.readDouble();
            this.setDouble(i, value);
        }
    }


}
