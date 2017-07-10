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

/**
 * An Array implementation designed to hold a dense array of String values encoded as a primitive char array
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class DenseArrayOfUtf16 extends ArrayBase<String> {

    private static final long serialVersionUID = 1L;

    private static final char[] zeroLength = new char[0];
    private static final long maxIntValueAsLong = (long)Integer.MAX_VALUE;

    private int[] widths;
    private char[][] data;
    private int maxWidth;
    private long maxWidthAsLong;
    private String defaultValue;

    /**
     * Constructor
     * @param length        the length for this array
     * @param defaultValue  the default value for array
     */
    DenseArrayOfUtf16(int length, String defaultValue) {
        super(String.class, ArrayStyle.DENSE, false);
        this.maxWidth = 10;  //todo: fix this
        this.maxWidthAsLong = maxWidth;
        this.defaultValue = defaultValue;
        this.widths = new int[length];
        this.maxWidth = defaultValue == null ? maxWidth : maxWidth > defaultValue.length() ? maxWidth : defaultValue.length();
        this.data = createArray(length, maxWidthAsLong);
        if (defaultValue == null) {
            Arrays.fill(widths, -1);
        } else {
            final char[] chars = defaultValue.toCharArray();
            Arrays.fill(widths, defaultValue.length());
            for (int i=0; i<length; ++i) {
                final int start = getStart(i);
                final char[] charArray = getSegment(i);
                System.arraycopy(chars, 0, charArray, start, chars.length);
            }
        }
    }


    /**
     * Constructor
     * @param source    the source array to copy
     * @param parallel  true for the parallel version
     */
    private DenseArrayOfUtf16(DenseArrayOfUtf16 source, boolean parallel) {
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
    private char[][] createArray(int capacity, long maxLength) {
        final long size = (long)capacity * maxLength;
        final int batchCount = (int)((size / maxIntValueAsLong) + 1);
        final int remainder = (int)(size % maxIntValueAsLong);
        final char[][] data = new char[batchCount][];
        for (int i=0; i<batchCount; ++i) {
            data[i] = new char[i == batchCount-1 ? remainder : (int) maxIntValueAsLong];
        }
        return data;
    }


    /**
     * Re-sizes the character array to be able to hold entries with a larger width
     * @param capacity      the capacity for this array
     * @param maxLength     the new max length for entries
     */
    private void resize(int capacity, long maxLength) {
        if (maxLength <= maxWidthAsLong) {
            throw new ArrayException("Attempt to resize to smaller string width for array");
        } else {
            final char[][] newData = createArray(capacity, maxLength);
            for (int index=0; index<capacity; ++index) {
                final int length = widths[index];
                if (length > 0) {
                    final long newIndex = (long)index * maxLength;
                    final long oldIndex = (long)index * maxWidthAsLong;
                    final int new_i = (int)(newIndex / maxIntValueAsLong);
                    final int new_j = (int)(newIndex % maxIntValueAsLong);
                    final int old_i = (int)(oldIndex / maxIntValueAsLong);
                    final int old_j = (int)(oldIndex % maxIntValueAsLong);
                    final char[] oldChars = data[old_i];
                    final char[] newChars = newData[new_i];
                    System.arraycopy(oldChars, old_j, newChars, new_j, length);
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
        return isParallel() ? this : new DenseArrayOfUtf16(this, true);
    }


    @Override
    public final Array<String> sequential() {
        return isParallel() ? new DenseArrayOfUtf16(this, false) : this;
    }


    @Override
    public final Array<String> copy() {
        try {
            final DenseArrayOfUtf16 copy = (DenseArrayOfUtf16)super.clone();
            copy.defaultValue = this.defaultValue;
            copy.maxWidth = this.maxWidth;
            copy.maxWidthAsLong = this.maxWidthAsLong;
            copy.widths = this.widths.clone();
            copy.data = new char[data.length][data[0].length];
            for (int i=0; i<data.length; ++i) {
                final char[] source = data[i];
                final char[] target = copy.data[i];
                System.arraycopy(source, 0, target, 0, source.length);
            }
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed to copy Array: " + this, ex);
        }
    }


    @Override
    public final Array<String> copy(int[] indexes) {
        final DenseArrayOfUtf16 clone = new DenseArrayOfUtf16(indexes.length, defaultValue);
        for (int i = 0; i < indexes.length; ++i) {
            final String value = this.getValue(indexes[i]);
            clone.setValue(i, value);
        }
        return clone;
    }


    @Override()
    public final Array<String> copy(int start, int end) {
        final int length = end - start;
        final DenseArrayOfUtf16 clone = new DenseArrayOfUtf16(length, defaultValue);
        for (int i=0; i<length; ++i) {
            final String value = getValue(start+i);
            clone.setValue(i, value);
        }
        return clone;
    }


    @Override
    protected final Array<String> sort(int start, int end, int multiplier) {
        return doSort(start, end, (int i, int j) -> multiplier * this.compare(i, j));
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
            final char[] segment1 = getSegment(i);
            final char[] segment2 = getSegment(j);
            while (k < width) {
                final char c1 = segment1[start1 + k];
                final char c2 = segment2[start2 + k];
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
        final char[] segment1 = getSegment(index1);
        final char[] segment2 = getSegment(j);
        this.widths[index1] = width2;
        this.widths[j] = width1;
        for (int i=0; i<maxWidth; ++i) {
            final int x = start1+i;
            final int y = start2+i;
            final char c1 = segment1[x];
            final char c2 = segment2[y];
            segment1[x] = c2;
            segment2[y] = c1;
        }
        return this;
    }


    @Override
    public final Array<String> filter(Predicate<ArrayValue<String>> predicate) {
        final ArrayCursor<String> cursor = cursor();
        final ArrayBuilder<String> builder = ArrayBuilder.of(length(), type());
        for (int i=0; i<length(); ++i) {
            cursor.moveTo(i);
            final boolean match = predicate.test(cursor);
            if (match) {
                builder.add(cursor.getValue());
            }
        }
        return builder.toArray();
    }


    @Override
    public final Array<String> update(Array<String> from, int[] fromIndexes, int[] toIndexes) {
        if (fromIndexes.length != toIndexes.length) {
            throw new ArrayException("The from index array must have the same length as the to index array");
        } else {
            if (from instanceof DenseArrayOfUtf16) {
                final DenseArrayOfUtf16 dense = (DenseArrayOfUtf16)from;
                for (int i=0; i<fromIndexes.length; ++i) {
                    final int toIndex = toIndexes[i];
                    final int fromIndex = fromIndexes[i];
                    final char[] update = dense.getChars(fromIndex);
                    this.expand(toIndex);
                    this.setChars(toIndex, update);
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
        if (from instanceof DenseArrayOfUtf16) {
            final DenseArrayOfUtf16 other = (DenseArrayOfUtf16)from;
            for (int i=0; i<length; ++i) {
                final char[] update = other.getChars(fromIndex + i);
                this.expand(toIndex + i);
                this.setChars(toIndex + i, update);
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
            final int[] newLengths = new int[newCapacity];
            System.arraycopy(widths, 0, newLengths, 0, widths.length);
            this.widths = newLengths;
            final long size = (long)newCapacity * maxWidthAsLong;
            final int batchCount = (int)((size / maxIntValueAsLong) + 1);
            final int remainder = (int)(size % maxIntValueAsLong);
            final char[][] oldData = this.data;
            this.data = new char[batchCount][];
            for (int i=0; i<batchCount; ++i) {
                data[i] = new char[i == batchCount-1 ? remainder : (int) maxIntValueAsLong];
                if (oldData.length > i) {
                    System.arraycopy(oldData[i], 0, data[i], 0, oldData[i].length);
                }
            }
            if (defaultValue == null) {
                Arrays.fill(widths, oldCapacity, newLength, -1);
            } else {
                final char[] chars = defaultValue.toCharArray();
                Arrays.fill(widths, oldCapacity, newLength, chars.length);
                for (int i=oldCapacity; i<newLength; ++i) {
                    final int start = getStart(i);
                    final char[] charArray = getSegment(i);
                    System.arraycopy(chars, 0, charArray, start, chars.length);
                }
            }
        }
        return this;
    }


    @Override()
    public final Array<String> fill(String value) {
        final char[] chars = value == null ? new char[0] : value.toCharArray();
        for (int i = 0; i< length(); ++i) {
            this.setChars(i, chars);
        }
        return this;
    }


    @Override
    public final Array<String> fill(String value, int start, int end) {
        final char[] chars = value == null ? new char[0] : value.toCharArray();
        for (int i = start; i< end; ++i) {
            this.setChars(i, chars);
        }
        return this;
    }


    @Override
    public final boolean isNull(int index) {
        return widths[index] < 0;
    }


    @Override
    public final boolean isEqualTo(int index, String value) {
        if (value == null) {
            return widths[index] < 0;
        } else {
            final char[] chars = value.toCharArray();
            if (chars.length != widths[index]) {
                return false;
            } else {
                final char[] other = getChars(index);
                return Arrays.equals(chars, other);
            }
        }
    }


    @Override
    public final String getValue(int index) {
        final char[] chars = getChars(index);
        if (chars == null) {
            return null;
        } else if (chars.length == 0) {
            return "";
        } else {
            return new String(chars);
        }
    }


    @Override
    public final String setValue(int index, String value) {
        final String oldValue = getValue(index);
        if (value == null) {
            this.setChars(index, null);
            return oldValue;
        } else if (value.length() == 0) {
            this.setChars(index, zeroLength);
            return oldValue;
        } else {
            final char[] chars = value.toCharArray();
            this.setChars(index, chars);
            return oldValue;
        }
    }


    /**
     * Returns the chars for the array entry at index specified
     * @param index the array index
     * @return      the char array for index, null if no value
     */
    private char[] getChars(int index) {
        final int length = widths[index];
        if (length < 0) {
            return null;
        } else if (length == 0) {
            return zeroLength;
        } else {
            final int start = getStart(index);
            final char[] charArray = getSegment(index);
            final char[] chars = new char[length];
            System.arraycopy(charArray, start, chars, 0, length);
            return chars;
        }
    }


    /**
     * Sets value at index as a char array
     * @param index     the array index
     * @param chars     the array of chars, can be null
     */
    private void setChars(int index, char[] chars) {
        if (chars == null) {
            this.widths[index] = -1;
        } else {
            if (chars.length > maxWidth) resize(length(), (long)chars.length);
            final int start = getStart(index);
            final char[] charArray = getSegment(index);
            System.arraycopy(chars, 0, charArray, start, chars.length);
            this.widths[index] = chars.length;
        }
    }


    /**
     * Returns the char array segment which contains the entry for index specified
     * @param index     the array index
     * @return          the internal array segment that contains entry for index
     */
    private char[] getSegment(int index) {
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
                final char[] chars = (char[])is.readObject();
                this.setChars(i, chars);
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
            final char[] chars = getChars(index);
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
        for (char[] chars : data) {
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
        this.data = new char[count][];
        for (int i=0; i<count; ++i) {
            data[i] = (char[])is.readObject();
        }
    }

}
