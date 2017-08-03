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

import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
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
 * An Array implementation designed to hold a sparse array of double values
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class SparseArrayOfDoubles extends ArrayBase<Double> {

    private static final long serialVersionUID = 1L;

    private int length;
    private TIntDoubleMap values;
    private double defaultValue;

    /**
     * Constructor
     * @param length    the length for this array
     * @param defaultValue  the default value for array
     */
    SparseArrayOfDoubles(int length, Double defaultValue) {
        super(Double.class, ArrayStyle.SPARSE, false);
        this.length = length;
        this.defaultValue = defaultValue != null ? defaultValue : Double.NaN;
        this.values = new TIntDoubleHashMap((int)Math.max(length * 0.5, 10d), 0.8f, -1, this.defaultValue);
    }

    /**
     * Constructor
     * @param source    the source array to shallow copy
     * @param parallel  true for the parallel version
     */
    private SparseArrayOfDoubles(SparseArrayOfDoubles source, boolean parallel) {
        super(source.type(), ArrayStyle.SPARSE, parallel);
        this.length = source.length;
        this.defaultValue = source.defaultValue;
        this.values = source.values;
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
    public final Double defaultValue() {
        return defaultValue;
    }


    @Override
    public final Array<Double> parallel() {
        return isParallel() ? this : new SparseArrayOfDoubles(this, true);
    }


    @Override
    public final Array<Double> sequential() {
        return isParallel() ? new SparseArrayOfDoubles(this, false) : this;
    }


    @Override()
    public final Array<Double> copy() {
        try {
            final SparseArrayOfDoubles copy = (SparseArrayOfDoubles)super.clone();
            copy.values = new TIntDoubleHashMap(values);
            copy.defaultValue = this.defaultValue;
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed to copy Array: " + this, ex);
        }
    }


    @Override()
    public final Array<Double> copy(int[] indexes) {
        final SparseArrayOfDoubles clone = new SparseArrayOfDoubles(indexes.length, defaultValue);
        for (int i = 0; i < indexes.length; ++i) {
            final double value = getDouble(indexes[i]);
            clone.setDouble(i, value);
        }
        return clone;
    }


    @Override()
    public final Array<Double> copy(int start, int end) {
        final int length = end - start;
        final SparseArrayOfDoubles clone = new SparseArrayOfDoubles(length, defaultValue);
        for (int i=0; i<length; ++i) {
            final double value = getDouble(start+i);
            if (Double.compare(value, defaultValue) != 0) {
                clone.setValue(i, value);
            }
        }
        return clone;
    }


    @Override
    protected final Array<Double> sort(int start, int end, int multiplier) {
        return doSort(start, end, (i, j) -> {
            final double v1 = values.get(i);
            final double v2 = values.get(j);
            return multiplier * Double.compare(v1, v2);
        });
    }


    @Override
    public final int compare(int i, int j) {
        return Double.compare(values.get(i), values.get(j));
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
        int count = 0;
        final int length = this.length();
        final ArrayCursor<Double> cursor = cursor();
        final Array<Double> matches = Array.of(type(), length, loadFactor());  //todo: fix the length of this filter
        for (int i=0; i<length; ++i) {
            cursor.moveTo(i);
            final boolean match = predicate.test(cursor);
            if (match) matches.setDouble(count++, cursor.getDouble());
        }
        return count == length ? matches : matches.copy(0, count);
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
        this.length = newLength > length ? newLength : length;
        return this;
    }


    @Override
    public Array<Double> fill(Double value, int start, int end) {
        final double fillValue = value == null ? defaultValue : value;
        if (fillValue == defaultValue) {
            this.values.clear();
        } else {
            for (int i=start; i<end; ++i) {
                this.values.put(i, fillValue);
            }
        }
        return this;
    }


    @Override
    public final boolean isNull(int index) {
        return Double.isNaN(values.get(index));
    }


    @Override
    public final boolean isEqualTo(int index, Double value) {
        return value == null || Double.isNaN(value) ? isNull(index) : value == values.get(index);
    }


    @Override
    public final double getDouble(int index) {
        this.checkBounds(index, length);
        return values.get(index);
    }


    @Override
    public final Double getValue(int index) {
        this.checkBounds(index, length);
        return values.get(index);
    }


    @Override
    public final double setDouble(int index, double value) {
        this.checkBounds(index, length);
        final double oldValue = getDouble(index);
        if (value == defaultValue) {
            this.values.remove(index);
            return oldValue;
        } else {
            this.values.put(index, value);
            return oldValue;
        }
    }


    @Override
    public final Double setValue(int index, Double value) {
        this.checkBounds(index, length);
        final Double oldValue = getValue(index);
        if (value == null || Double.compare(value, defaultValue) == 0) {
            this.values.remove(index);
            return oldValue;
        } else {
            this.values.put(index, value);
            return oldValue;
        }
    }


    @Override
    public final int binarySearch(int start, int end, Double value) {
        int low = start;
        int high = end - 1;
        while (low <= high) {
            final int midIndex = (low + high) >>> 1;
            final double midValue = getDouble(midIndex);
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
            final double current = values.get(i);
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

}
