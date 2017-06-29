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
package com.zavtech.morpheus.array.tasks;

import java.util.Comparator;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayException;
import com.zavtech.morpheus.util.Bounds;
import com.zavtech.morpheus.util.Comparators;

/**
 * A RecursiveTask implementation that yields the max value in a Morpheus Array using the Fork and Join Framework.
 *
 * @param <T>   the array type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class BoundsTask<T> extends RecursiveTask<Bounds<T>> {

    private Array<T> array;
    private int from, to, splitThreshold;

    /**
     * Constructor
     * @param array     the array to operate on
     * @param from      the from index in array, inclusive
     * @param to        the to index in array, inclusive
     * @param splitThreshold    the split threshold, below which not to split further
     */
    @SuppressWarnings("unchecked")
    public BoundsTask(Array<T> array, int from, int to, int splitThreshold) {
        this.array = array;
        this.from = from;
        this.to = to;
        this.splitThreshold = splitThreshold;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Bounds<T> compute() {
        try {
            if (array.length() == 0) return null;
            final int length = to - from + 1;
            if (length > splitThreshold) {
                final int splitLength = (to - from) / 2;
                final int midPoint = from + splitLength;
                final ForkJoinTask<Bounds<T>> left = new BoundsTask<>(array, from, midPoint, splitThreshold).fork();
                final BoundsTask<T> right = new BoundsTask<>(array, midPoint + 1, to, splitThreshold);
                final Bounds<T> x = right.compute();
                final Bounds<T> y = left.join();
                return Bounds.ofAll(x, y);
            } else {
                switch (array.typeCode()) {
                    case BOOLEAN:           return booleanBounds();
                    case INTEGER:           return intBounds();
                    case LONG:              return longBounds();
                    case DOUBLE:            return doubleBounds();
                    case CURRENCY:          return intCodingBounds();
                    case ZONE_ID:           return intCodingBounds();
                    case TIME_ZONE:         return intCodingBounds();
                    case ENUM:              return intCodingBounds();
                    case YEAR:              return intCodingBounds();
                    case DATE:              return longCodingBounds();
                    case INSTANT:           return longCodingBounds();
                    case LOCAL_DATE:        return longCodingBounds();
                    case LOCAL_TIME:        return longCodingBounds();
                    case LOCAL_DATETIME:    return longCodingBounds();
                    case ZONED_DATETIME:    return longCodingBounds();
                    default:                return bounds();
                }
            }
        } catch (ArrayException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ArrayException("Failed to compute Array bounds value", ex);
        }
    }


    /**
     * Returns the min value in the range for this task
     * @return  the min value in range
     */
    @SuppressWarnings("unchecked")
    private Bounds<T> booleanBounds() {
        boolean minValue = true;
        boolean maxValue = false;
        for (int i = from; i <= to; ++i) {
            final boolean value = array.getBoolean(i);
            minValue = Boolean.compare(value, minValue) < 0 ? value : minValue;
            maxValue = Boolean.compare(value, maxValue) > 0 ? value : maxValue;
        }
        return (Bounds<T>)Bounds.of(minValue, maxValue);
    }


    /**
     * Returns the min value in the range for this task
     * @return  the min value in range
     */
    @SuppressWarnings("unchecked")
    private Bounds<T> intBounds() {
        int minValue = Integer.MAX_VALUE;
        int maxValue = Integer.MIN_VALUE;
        for (int i = from; i <= to; ++i) {
            final int value = array.getInt(i);
            minValue = Integer.compare(value, minValue) < 0 ? value : minValue;
            maxValue = Integer.compare(value, maxValue) > 0 ? value : maxValue;
        }
        return (Bounds<T>)Bounds.of(minValue, maxValue);
    }


    /**
     * Returns the min value in the range for this task
     * @return  the min value in range
     */
    @SuppressWarnings("unchecked")
    private Bounds<T> longBounds() {
        long minValue = Long.MAX_VALUE;
        long maxValue = Long.MIN_VALUE;
        for (int i = from; i <= to; ++i) {
            final long value = array.getLong(i);
            minValue = Long.compare(value, minValue) < 0 ? value : minValue;
            maxValue = Long.compare(value, maxValue) > 0 ? value : maxValue;
        }
        return (Bounds<T>)Bounds.of(minValue, maxValue);
    }


    /**
     * Returns the min value in the range for this task
     * @return  the min value in range
     */
    @SuppressWarnings("unchecked")
    private Bounds<T> doubleBounds() {
        int count = 0;
        double minValue = Double.MAX_VALUE;
        double maxValue = Double.MIN_VALUE;
        for (int i = from; i <= to; ++i) {
            if (!array.isNull(i)) {
                count++;
                final double doubleValue = array.getDouble(i);
                minValue = Double.compare(doubleValue, minValue) < 0 ? doubleValue : minValue;
                maxValue = Double.compare(doubleValue, maxValue) > 0 ? doubleValue : maxValue;
            }
        }
        return count == 0 ? null : (Bounds<T>)Bounds.of(minValue, maxValue);
    }


    /**
     * Returns the min value in the range for this task
     * @return  the min value in range
     */
    private Bounds<T> bounds() {
        int count = 0;
        T minValue = null;
        T maxValue = null;
        final Class<T> type = array.type();
        final Comparator<T> comparator = Comparators.getDefaultComparator(type);
        for (int i = from; i <= to; ++i) {
            if (!array.isNull(i)) {
                count++;
                final T value = array.getValue(i);
                minValue = minValue == null ? value : comparator.compare(value, minValue) < 0 ? value : minValue;
                maxValue = maxValue == null ? value : comparator.compare(value, maxValue) > 0 ? value : maxValue;
            }
        }
        return count == 0 ? null : Bounds.of(minValue, maxValue);
    }


    /**
     * Returns the min value in the range for this task
     * @return  the min value in range
     */
    private Bounds<T> intCodingBounds() {
        int minIndex = -1;
        int maxIndex = -1;
        int minValue = Integer.MAX_VALUE;
        int maxValue = Integer.MIN_VALUE;
        for (int i = from; i <= to; ++i) {
            if (!array.isNull(i)) {
                final int value = array.getInt(i);
                if (Integer.compare(value, minValue) < 0) {
                    minValue = value;
                    minIndex = i;
                }
                if (Integer.compare(value, maxValue) > 0) {
                    maxValue = value;
                    maxIndex = i;
                }
            }
        }
        if (minIndex < 0) {
            return null;
        } else {
            final T min = array.getValue(minIndex);
            final T max = array.getValue(maxIndex);
            return Bounds.of(min, max);
        }
    }


    /**
     * Returns the min value in the range for this task
     * @return  the min value in range
     */
    private Bounds<T> longCodingBounds() {
        int minIndex = -1;
        int maxIndex = -1;
        long minValue = Long.MAX_VALUE;
        long maxValue = Long.MIN_VALUE;
        for (int i = from; i <= to; ++i) {
            if (!array.isNull(i)) {
                final long value = array.getLong(i);
                if (Long.compare(value, minValue) < 0) {
                    minValue = value;
                    minIndex = i;
                }
                if (Long.compare(value, maxValue) > 0) {
                    maxValue = value;
                    maxIndex = i;
                }
            }
        }
        if (minIndex < 0) {
            return null;
        } else {
            final T min = array.getValue(minIndex);
            final T max = array.getValue(maxIndex);
            return Bounds.of(min, max);
        }
    }
}
