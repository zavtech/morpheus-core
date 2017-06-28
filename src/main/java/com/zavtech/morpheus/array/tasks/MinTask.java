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
import com.zavtech.morpheus.util.Comparators;

/**
 * A RecursiveTask implementation that yields the min value in a Morpheus Array using the Fork & Join Framework.
 *
 * @param <T>   the array type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class MinTask<T> extends RecursiveTask<T> {

    private Array<T> array;
    private Comparator<T> comparator;
    private int from, to, splitThreshold;

    /**
     * Constructor
     * @param array     the array to operate on
     * @param from      the from index in array, inclusive
     * @param to        the to index in array, inclusive
     * @param splitThreshold    the split threshold, below which not to split further
     */
    @SuppressWarnings("unchecked")
    public MinTask(Array<T> array, int from, int to, int splitThreshold) {
        this.array = array;
        this.from = from;
        this.to = to;
        this.splitThreshold = splitThreshold;
        this.comparator = Comparators.getDefaultComparator(array.type());
    }

    @Override
    @SuppressWarnings("unchecked")
    public T compute() {
        try {
            if (array.length() == 0) return null;
            final int length = to - from + 1;
            if (length > splitThreshold) {
                final int splitLength = (to - from) / 2;
                final int midPoint = from + splitLength;
                final ForkJoinTask<T> left = new MinTask<>(array, from, midPoint, splitThreshold).fork();
                final MinTask<T> right = new MinTask<>(array, midPoint + 1, to, splitThreshold);
                final T x = right.compute();
                final T y = left.join();
                return comparator.compare(x, y) > 0 ? y : x;
            } else {
                switch (array.typeCode()) {
                    case BOOLEAN:           return (T)minBoolean();
                    case INTEGER:           return (T)minInt();
                    case LONG:              return (T)minLong();
                    case DOUBLE:            return (T)minDouble();
                    case CURRENCY:          return minIntCoding();
                    case ZONE_ID:           return minIntCoding();
                    case TIME_ZONE:         return minIntCoding();
                    case ENUM:              return minIntCoding();
                    case YEAR:              return minIntCoding();
                    case DATE:              return minLongCoding();
                    case INSTANT:           return minLongCoding();
                    case LOCAL_DATE:        return minLongCoding();
                    case LOCAL_TIME:        return minLongCoding();
                    case LOCAL_DATETIME:    return minLongCoding();
                    case ZONED_DATETIME:    return minLongCoding();
                    default:                return minValue();
                }
            }
        } catch (ArrayException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ArrayException("Failed to compute Array min value", ex);
        }
    }


    /**
     * Returns the min value in the range for this task
     * @return  the min value in range
     */
    private Boolean minBoolean() {
        boolean minValue = true;
        for (int i = from; i <= to; ++i) {
            final boolean value = array.getBoolean(i);
            minValue = Boolean.compare(value, minValue) < 0 ? value : minValue;
        }
        return minValue;
    }


    /**
     * Returns the min value in the range for this task
     * @return  the min value in range
     */
    private Integer minInt() {
        int minValue = Integer.MAX_VALUE;
        for (int i = from; i <= to; ++i) {
            final int value = array.getInt(i);
            minValue = Integer.compare(value, minValue) < 0 ? value : minValue;
        }
        return minValue;
    }


    /**
     * Returns the min value in the range for this task
     * @return  the min value in range
     */
    private Long minLong() {
        long minValue = Long.MAX_VALUE;
        for (int i = from; i <= to; ++i) {
            final long value = array.getLong(i);
            minValue = Long.compare(value, minValue) < 0 ? value : minValue;
        }
        return minValue;
    }


    /**
     * Returns the min value in the range for this task
     * @return  the min value in range
     */
    private Double minDouble() {
        int count = 0;
        double minValue = Double.MAX_VALUE;
        for (int i = from; i <= to; ++i) {
            if (!array.isNull(i)) {
                count++;
                final double value = array.getDouble(i);
                minValue = Double.compare(value, minValue) < 0 ? value : minValue;
            }
        }
        return count == 0 ? null : minValue;
    }


    /**
     * Returns the min value in the range for this task
     * @return  the min value in range
     */
    private T minValue() {
        T minValue = null;
        for (int i=0; i < array.length(); ++i) {
            if (!array.isNull(i)) {
                final T value = array.getValue(i);
                minValue = minValue == null ? value : comparator.compare(value, minValue) < 0 ? value : minValue;
            }
        }
        return minValue;
    }


    /**
     * Returns the min value in the range for this task
     * @return  the min value in range
     */
    private T minIntCoding() {
        int minIndex = -1;
        int minValue = Integer.MAX_VALUE;
        for (int i = from; i <= to; ++i) {
            if (!array.isNull(i)) {
                final int value = array.getInt(i);
                if (Integer.compare(value, minValue) < 0) {
                    minValue = value;
                    minIndex = i;
                }
            }
        }
        return minIndex < 0 ? null : array.getValue(minIndex);
    }


    /**
     * Returns the min value in the range for this task
     * @return  the min value in range
     */
    private T minLongCoding() {
        int minIndex = -1;
        long minValue = Long.MAX_VALUE;
        for (int i = from; i <= to; ++i) {
            if (!array.isNull(i)) {
                final long value = array.getLong(i);
                if (Long.compare(value, minValue) < 0) {
                    minValue = value;
                    minIndex = i;
                }
            }
        }
        return minIndex < 0 ? null : array.getValue(minIndex);
    }

}
