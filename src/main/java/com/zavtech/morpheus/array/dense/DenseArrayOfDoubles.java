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

import gnu.trove.set.TDoubleSet;
import gnu.trove.set.hash.TDoubleHashSet;

import com.zavtech.morpheus.array.ArrayBuilder;
import com.zavtech.morpheus.array.ArrayCursor;
import com.zavtech.morpheus.array.ArrayException;
import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBase;
import com.zavtech.morpheus.array.ArrayStyle;
import com.zavtech.morpheus.array.ArrayValue;

/**
 * An Array implementation designed to hold a dense array of double values
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class DenseArrayOfDoubles extends ArrayBase<Double> {

    private static final long serialVersionUID = 1L;

    private double[] values;
    private final double defaultValue;

    /**
     * Constructor
     * @param length        the length for this array
     * @param defaultValue  the default value for array
     */
    DenseArrayOfDoubles(int length, Double defaultValue) {
        super(Double.class, ArrayStyle.DENSE, false);
        this.values = new double[length];
        this.defaultValue = defaultValue != null ? defaultValue : Double.NaN;
        Arrays.fill(values, this.defaultValue);
    }

    /**
     * Constructor
     * @param source    the source array to shallow copy
     * @param parallel  true for parallel version
     */
    private DenseArrayOfDoubles(DenseArrayOfDoubles source, boolean parallel) {
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
    public final Double defaultValue() {
        return defaultValue;
    }


    @Override
    public final Array<Double> parallel() {
        return isParallel() ? this : new DenseArrayOfDoubles(this, true);
    }


    @Override
    public final Array<Double> sequential() {
        return isParallel() ? new DenseArrayOfDoubles(this, false) : this;
    }


    @Override()
    public final Array<Double> copy() {
        try {
            final DenseArrayOfDoubles copy = (DenseArrayOfDoubles)super.clone();
            copy.values = this.values.clone();
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed to copy Array: " + this, ex);
        }
    }


    @Override()
    public final Array<Double> copy(int[] indexes) {
        final DenseArrayOfDoubles clone = new DenseArrayOfDoubles(indexes.length, defaultValue);
        for (int i = 0; i < indexes.length; ++i) {
            clone.values[i] = this.values[indexes[i]];
        }
        return clone;
    }


    @Override()
    public final Array<Double> copy(int start, int end) {
        final int length = end - start;
        final DenseArrayOfDoubles clone = new DenseArrayOfDoubles(length, defaultValue);
        System.arraycopy(values, start, clone.values, 0, length);
        return clone;
    }


    @Override
    protected final Array<Double> sort(int start, int end, int multiplier) {
        return doSort(start, end, (i, j) -> multiplier * Double.compare(values[i], values[j]));
    }


    @Override
    public final int compare(int i, int j) {
        return Double.compare(values[i], values[j]);
    }


    @Override
    public final Array<Double> swap(int i, int j) {
        final double v1 = values[i];
        final double v2 = values[j];
        this.values[i] = v2;
        this.values[j] = v1;
        return this;
    }


    @Override
    public final Array<Double> filter(Predicate<ArrayValue<Double>> predicate) {
        final ArrayCursor<Double> cursor = cursor();
        final ArrayBuilder<Double> builder = ArrayBuilder.of(length(), type());
        for (int i=0; i<values.length; ++i) {
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
        if (newLength > values.length) {
            final double[] newValues = new double[newLength];
            System.arraycopy(values, 0, newValues, 0, values.length);
            Arrays.fill(newValues, values.length, newValues.length, defaultValue);
            this.values = newValues;
        }
        return this;
    }


    @Override
    public final Array<Double> fill(Double value, int start, int end) {
        Arrays.fill(values, start, end, value == null ? defaultValue : value);
        return this;
    }


    @Override
    public final boolean isNull(int index) {
        return Double.isNaN(values[index]);
    }


    @Override
    public final boolean isEqualTo(int index, Double value) {
        return value == null || Double.isNaN(value) ? Double.isNaN(values[index]) : values[index] == value;
    }


    @Override
    public final double getDouble(int index) {
        return values[index];
    }


    @Override
    public final Double getValue(int index) {
        return values[index];
    }


    @Override
    public final double setDouble(int index, double value) {
        final double oldValue = getDouble(index);
        this.values[index] = value;
        return oldValue;
    }


    @Override
    public final Double setValue(int index, Double value) {
        final Double oldValue = getValue(index);
        this.values[index] = value != null ? value : Double.NaN;
        return oldValue;
    }


    @Override
    public final int binarySearch(int start, int end, Double value) {
        return Arrays.binarySearch(values, start, end, value);
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
        result.setDouble(0, values[0]);
        for (int i=1; i<length; ++i) {
            final double prior = result.getDouble(i-1);
            final double current = values[i];
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
            this.values[i] = is.readDouble();
        }
    }


    @Override
    public final void write(ObjectOutputStream os, int[] indexes) throws IOException {
        for (int index : indexes) {
            os.writeDouble(values[index]);
        }
    }

    /** Custom serialization */
    private void writeObject(ObjectOutputStream os) throws IOException {
        os.writeInt(values.length);
        for (double value : values) {
            os.writeDouble(value);
        }
    }


    @SuppressWarnings("unchecked")
    /** Custom serialization */
    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
        final int length = is.readInt();
        this.values = new double[length];
        for (int i=0; i<length; ++i) {
            values[i] = is.readDouble();
        }
    }

}
