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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Predicate;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBase;
import com.zavtech.morpheus.array.ArrayBuilder;
import com.zavtech.morpheus.array.ArrayException;
import com.zavtech.morpheus.array.ArrayStyle;

/**
 * An Array implementation designed to hold a dense array of String values stored as a UTF-8 encoded byte array.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class DenseArrayOfUtf8 extends ArrayBase<String> {

    private static final long serialVersionUID = 1L;

    private static final long maxIntValueAsLong = (long)Integer.MAX_VALUE;

    private static final Charset utf8 = StandardCharsets.UTF_8;

    private int[] widths;
    private byte[][] data;
    private int maxWidth;
    private long maxWidthAsLong;
    private String defaultValue;

    /**
     * Constructor
     * @param length        the length for this array
     * @param defaultValue  the default value for array
     */
    DenseArrayOfUtf8(int length, String defaultValue) {
        super(String.class, ArrayStyle.DENSE, false);
        this.maxWidth = 10; // todo: fix this
        this.maxWidthAsLong = maxWidth;
        this.widths = new int[length];
        this.defaultValue = defaultValue;
        this.maxWidth = defaultValue == null ? maxWidth : maxWidth > defaultValue.length() ? maxWidth : defaultValue.length();
        this.data = createArray(length, maxWidthAsLong);
        if (defaultValue == null) {
            Arrays.fill(widths, -1);
        } else {
            final byte[] bytes = defaultValue.getBytes(utf8);
            Arrays.fill(widths, defaultValue.length());
            for (int i=0; i<length; ++i) {
                final int start = getStart(i);
                final byte[] segment = getSegment(i);
                System.arraycopy(bytes, 0, segment, start, bytes.length);
            }
        }
    }


    /**
     * Constructor
     * @param source        the source array to shallow copy
     * @param parallel      true for the parallel version
     */
    private DenseArrayOfUtf8(DenseArrayOfUtf8 source, boolean parallel) {
        super(source.type(), ArrayStyle.DENSE, parallel);
        this.maxWidth = source.maxWidth;
        this.maxWidthAsLong = source.maxWidthAsLong;
        this.defaultValue = source.defaultValue;
        this.widths = source.widths;
        this.data = source.data;
    }


    /**
     * Returns a newly created array with capacity and max length specified
     * @param capacity      the capacity for array
     * @param maxLength     the max length of a string entry
     * @return              the newly created array
     */
    private byte[][] createArray(int capacity, long maxLength) {
        final long size = (long)capacity * maxLength;
        final int batchCount = (int)((size / maxIntValueAsLong) + 1);
        final int remainder = (int)(size % maxIntValueAsLong);
        final byte[][] data = new byte[batchCount][];
        for (int i=0; i<batchCount; ++i) {
            data[i] = new byte[i == batchCount-1 ? remainder : (int) maxIntValueAsLong];
        }
        return data;
    }


    /**
     * Re-sizes the data array to be able to hold entries with a larger width
     * @param capacity      the capacity for this array
     * @param maxLength     the new max length for entries
     */
    private void resize(int capacity, long maxLength) {
        if (maxLength <= maxWidthAsLong) {
            throw new ArrayException("Attempt to resize to smaller string width for array");
        } else {
            final byte[][] newData = createArray(capacity, maxLength);
            for (int index=0; index<capacity; ++index) {
                final int length = widths[index];
                if (length > 0) {
                    final long newIndex = (long)index * maxLength;
                    final long oldIndex = (long)index * maxWidthAsLong;
                    final int new_i = (int)(newIndex / maxIntValueAsLong);
                    final int new_j = (int)(newIndex % maxIntValueAsLong);
                    final int old_i = (int)(oldIndex / maxIntValueAsLong);
                    final int old_j = (int)(oldIndex % maxIntValueAsLong);
                    final byte[] oldBytes = data[old_i];
                    final byte[] newBytes = newData[new_i];
                    System.arraycopy(oldBytes, old_j, newBytes, new_j, length);
                }
            }
            this.data = newData;
            this.maxWidth = (int)maxLength;
            this.maxWidthAsLong = maxLength;
        }
    }


    @Override
    public final int length() {
        return widths.length;
    }


    @Override
    public float loadFactor() {
        return 1F;
    }


    @Override
    public final String defaultValue() {
        return defaultValue;
    }


    @Override
    public final Array<String> parallel() {
        return isParallel() ? this : new DenseArrayOfUtf8(this, true);
    }


    @Override
    public final Array<String> sequential() {
        return isParallel() ? new DenseArrayOfUtf8(this, false) : this;
    }


    @Override
    public final Array<String> copy() {
        try {
            final DenseArrayOfUtf8 copy = (DenseArrayOfUtf8)super.clone();
            copy.defaultValue = this.defaultValue;
            copy.maxWidth = this.maxWidth;
            copy.maxWidthAsLong = this.maxWidthAsLong;
            copy.widths = this.widths.clone();
            copy.data = new byte[data.length][data[0].length];
            for (int i=0; i<data.length; ++i) {
                final byte[] source = data[i];
                final byte[] target = copy.data[i];
                System.arraycopy(source, 0, target, 0, source.length);
            }
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed to copy Array: " + this, ex);
        }
    }


    @Override
    public final Array<String> copy(int[] indexes) {
        final DenseArrayOfUtf8 clone = new DenseArrayOfUtf8(indexes.length, defaultValue);
        for (int i = 0; i < indexes.length; ++i) {
            final byte[] bytes = this.getBytes(indexes[i]);
            clone.setBytes(i, bytes);
        }
        return clone;
    }


    @Override()
    public final Array<String> copy(int start, int end) {
        final int length = end - start;
        final DenseArrayOfUtf8 clone = new DenseArrayOfUtf8(length, defaultValue);
        for (int i=0; i<length; ++i) {
            final byte[] bytes = getBytes(start+i);
            clone.setBytes(i, bytes);
        }
        return clone;
    }


    @Override
    protected final Array<String> sort(int start, int end, int multiplier) {
        return doSort(start, end,(i, j) -> multiplier * this.compare(i, j));
    }


    @Override
    public final int compare(int i, int j) {
        final int width1 = widths[i];
        final int width2 = widths[j];
        final int width = Math.min(width1, width2);
        if (width == 0) {
            return width1 - width2;
        } else {
            int k = 0;
            final int start1 = getStart(i);
            final int start2 = getStart(j);
            final byte[] segment1 = getSegment(i);
            final byte[] segment2 = getSegment(j);
            while (k < width) {
                final byte c1 = segment1[start1 + k];
                final byte c2 = segment2[start2 + k];
                if (c1 != c2) {
                    return (c1 - c2);
                }
                k++;
            }
            return (width1 - width2);
        }
    }


    @Override
    public final Array<String> swap(int index1, int j) {
        final int width1 = widths[index1];
        final int width2 = widths[j];
        final int start1 = getStart(index1);
        final int start2 = getStart(j);
        final byte[] segment1 = getSegment(index1);
        final byte[] segment2 = getSegment(j);
        this.widths[index1] = width2;
        this.widths[j] = width1;
        for (int i=0; i<maxWidth; ++i) {
            final int x = start1+i;
            final int y = start2+i;
            final byte c1 = segment1[x];
            final byte c2 = segment2[y];
            segment1[x] = c2;
            segment2[y] = c1;
        }
        return this;
    }


    @Override
    public final Array<String> filter(Predicate<String> predicate) {
        final ArrayBuilder<String> builder = ArrayBuilder.of(length(), type());
        for (int i=0; i<length(); ++i) {
            final String value = getValue(i);
            final boolean match = predicate.test(value);
            if (match) {
                builder.add(value);
            }
        }
        return builder.toArray();
    }


    @Override
    public final Array<String> update(Array<String> from, int[] fromIndexes, int[] toIndexes) {
        if (fromIndexes.length != toIndexes.length) {
            throw new ArrayException("The from index array must have the same length as the to index array");
        } else {
            if (from instanceof DenseArrayOfUtf8) {
                final DenseArrayOfUtf8 dense = (DenseArrayOfUtf8)from;
                for (int i=0; i<fromIndexes.length; ++i) {
                    final int toIndex = toIndexes[i];
                    final int fromIndex = fromIndexes[i];
                    final byte[] update = dense.getBytes(fromIndex);
                    this.expand(toIndex);
                    this.setBytes(toIndex, update);
                }
            } else {
                for (int i=0; i<fromIndexes.length; ++i) {
                    final int toIndex = toIndexes[i];
                    final int fromIndex = fromIndexes[i];
                    final String update = from.getValue(fromIndex);
                    this.expand(toIndex);
                    this.setValue(toIndex, update);
                }
            }
        }
        return this;
    }


    @Override
    public final Array<String> update(int toIndex, Array<String> from, int fromIndex, int length) {
        if (from instanceof DenseArrayOfUtf8) {
            final DenseArrayOfUtf8 other = (DenseArrayOfUtf8)from;
            for (int i=0; i<length; ++i) {
                final byte[] update = other.getBytes(fromIndex + i);
                this.expand(toIndex + i);
                this.setBytes(toIndex + i, update);
            }
        } else {
            for (int i=0; i<length; ++i) {
                final String update = from.getValue(fromIndex + i);
                this.expand(toIndex + i);
                this.setValue(toIndex + i, update);
            }
        }
        return this;
    }


    @Override
    public final Array<String> expand(int newLength) {
        if (newLength > widths.length) {
            final int oldCapacity = widths.length;
            int newCapacity = oldCapacity + (oldCapacity >> 1);
            if (newCapacity < newLength) newCapacity = newLength;
            final int[] newWidths = new int[newCapacity];
            System.arraycopy(widths, 0, newWidths, 0, widths.length);
            this.widths = newWidths;
            final long size = (long)newCapacity * maxWidthAsLong;
            final int batchCount = (int)((size / maxIntValueAsLong) + 1);
            final int remainder = (int)(size % maxIntValueAsLong);
            final byte[][] oldData = this.data;
            this.data = new byte[batchCount][];
            for (int i=0; i<batchCount; ++i) {
                data[i] = new byte[i == batchCount-1 ? remainder : (int) maxIntValueAsLong];
                if (oldData.length > i) {
                    System.arraycopy(oldData[i], 0, data[i], 0, oldData[i].length);
                }
            }
            if (defaultValue == null) {
                Arrays.fill(widths, oldCapacity, newLength, -1);
            } else {
                final byte[] bytes = defaultValue.getBytes(utf8);
                Arrays.fill(widths, oldCapacity, newLength, bytes.length);
                for (int i=oldCapacity; i<newLength; ++i) {
                    final int start = getStart(i);
                    final byte[] segment = getSegment(i);
                    System.arraycopy(bytes, 0, segment, start, bytes.length);
                }
            }
        }
        return this;
    }


    @Override()
    public final Array<String> fill(String value) {
        final byte[] bytes = value == null ? new byte[0] : value.getBytes();
        for (int i = 0; i< length(); ++i) {
            this.setBytes(i, bytes);
        }
        return this;
    }


    @Override
    public Array<String> fill(String value, int start, int end) {
        final byte[] bytes = value == null ? new byte[0] : value.getBytes();
        for (int i = start; i< end; ++i) {
            this.setBytes(i, bytes);
        }
        return this;
    }


    @Override
    public boolean isNull(int index) {
        return widths[index] < 0;
    }


    @Override
    public boolean isEqualTo(int index, String value) {
        if (value == null) {
            return widths[index] < 0;
        } else {
            final byte[] bytes = value.getBytes(utf8);
            if (bytes.length != widths[index]) {
                return false;
            } else {
                final byte[] other = getBytes(index);
                return Arrays.equals(bytes, other);
            }
        }
    }


    @Override
    public final String getValue(int index) {
        final byte[] bytes = getBytes(index);
        return bytes == null ? null : new String(bytes, utf8);
    }


    @Override
    public final String setValue(int index, String value) {
        final String oldValue = getValue(index);
        if (value == null) {
            this.setBytes(index, null);
            return oldValue;
        } else {
            final byte[] bytes = value.getBytes(utf8);
            this.setBytes(index, bytes);
            return oldValue;
        }
    }


    /**
     * Returns the bytes for the array entry at index specified
     * @param index the array index
     * @return      the bytes for index, null if no value
     */
    private byte[] getBytes(int index) {
        final int length = widths[index];
        if (length < 0) {
            return null;
        } else {
            final int start = getStart(index);
            final byte[] byteArray = getSegment(index);
            final byte[] bytes = new byte[length];
            System.arraycopy(byteArray, start, bytes, 0, length);
            return bytes;
        }
    }


    /**
     * Sets value at index as a byte array
     * @param index     the array index
     * @param bytes     the array of bytes, can be null
     */
    private void setBytes(int index, byte[] bytes) {
        if (bytes == null) {
            this.widths[index] = -1;
        } else {
            if (bytes.length > maxWidth) {
                resize(length(), (long)bytes.length);
            }
            final int start = getStart(index);
            final byte[] segment = getSegment(index);
            System.arraycopy(bytes, 0, segment, start, bytes.length);
            this.widths[index] = bytes.length;
        }
    }


    /**
     * Returns the char array segment which contains the entry for index specified
     * @param index     the array index
     * @return          the internal array segment that contains entry for index
     */
    private byte[] getSegment(int index) {
        final long longIndex = (long)index * maxWidthAsLong;
        final int i = (int)(longIndex / maxIntValueAsLong);
        return data[i];
    }


    /**
     * Returns the start index for the array index specified
     * @param index     the array index
     * @return          the start index in big char array
     */
    private int getStart(int index) {
        final long longIndex = (long)index * maxWidthAsLong;
        return (int)(longIndex % maxIntValueAsLong);
    }


    @Override
    public final void read(ObjectInputStream is, int count) throws IOException {
        try {
            final int length = is.readInt();
            this.maxWidth = is.readInt();
            this.maxWidthAsLong = is.readLong();
            this.defaultValue = (String)is.readObject();
            this.data = createArray(length, maxWidth);
            for (int i=0; i<count; ++i) {
                final byte[] chars = (byte[])is.readObject();
                this.setBytes(i, chars);
            }
        } catch (ClassNotFoundException ex) {
            throw new ArrayException("Failed to de-serialized array", ex);
        }
    }


    @Override
    public final void write(ObjectOutputStream os, int[] indexes) throws IOException {
        os.writeInt(indexes.length);
        os.writeInt(maxWidth);
        os.writeLong(maxWidthAsLong);
        os.writeObject(defaultValue);
        for (int index : indexes) {
            final byte[] chars = getBytes(index);
            os.writeObject(chars);
        }
    }


    /** Custom serialization */
    private void writeObject(ObjectOutputStream os) throws IOException {
        os.writeInt(maxWidth);
        os.writeLong(maxWidthAsLong);
        os.writeObject(defaultValue);
        os.writeObject(widths);
        os.writeInt(data.length);
        for (byte[] chars : data) {
            os.writeObject(chars);
        }
    }


    @SuppressWarnings("unchecked")
    /** Custom serialization */
    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
        this.maxWidth = is.readInt();
        this.maxWidthAsLong = is.readLong();
        this.defaultValue = (String)is.readObject();
        this.widths = (int[])is.readObject();
        final int count = is.readInt();
        this.data = new byte[count][];
        for (int i=0; i<count; ++i) {
            data[i] = (byte[])is.readObject();
        }
    }

}
