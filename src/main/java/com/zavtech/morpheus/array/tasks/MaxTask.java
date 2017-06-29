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
 * A RecursiveTask implementation that yields the max value in a Morpheus Array using the Fork & Join Framework.
 *
 * @param <T>   the array type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class MaxTask<T> extends RecursiveTask<T> {

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
    public MaxTask(Array<T> array, int from, int to, int splitThreshold) {
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
                final ForkJoinTask<T> left = new MaxTask<>(array, from, midPoint, splitThreshold).fork();
                final MaxTask<T> right = new MaxTask<>(array, midPoint + 1, to, splitThreshold);
                final T x = right.compute();
                final T y = left.join();
                return comparator.compare(x, y) > 0 ? x : y;
            } else {
                switch (array.typeCode()) {
                    case BOOLEAN:           return (T) maxBoolean();
                    case INTEGER:           return (T) maxInt();
                    case LONG:              return (T) maxLong();
                    case DOUBLE:            return (T) maxDouble();
                    case CURRENCY:          return maxIntCoding();
                    case ZONE_ID:           return maxIntCoding();
                    case TIME_ZONE:         return maxIntCoding();
                    case ENUM:              return maxIntCoding();
                    case YEAR:              return maxIntCoding();
                    case DATE:              return maxLongCoding();
                    case INSTANT:           return maxLongCoding();
                    case LOCAL_DATE:        return maxLongCoding();
                    case LOCAL_TIME:        return maxLongCoding();
                    case LOCAL_DATETIME:    return maxLongCoding();
                    case ZONED_DATETIME:    return maxLongCoding();
                    default:                return maxValue();
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
    private Boolean maxBoolean() {
        boolean maxValue = false;
        for (int i = from; i <= to; ++i) {
            final boolean value = array.getBoolean(i);
            maxValue = Boolean.compare(value, maxValue) > 0 ? value : maxValue;
        }
        return maxValue;
    }


    /**
     * Returns the min value in the range for this task
     * @return  the min value in range
     */
    private Integer maxInt() {
        int maxValue = Integer.MIN_VALUE;
        for (int i = from; i <= to; ++i) {
            final int value = array.getInt(i);
            maxValue = Integer.compare(value, maxValue) > 0 ? value : maxValue;
        }
        return maxValue;
    }


    /**
     * Returns the min value in the range for this task
     * @return  the min value in range
     */
    private Long maxLong() {
        long maxValue = Long.MIN_VALUE;
        for (int i = from; i <= to; ++i) {
            final long value = array.getLong(i);
            maxValue = Long.compare(value, maxValue) > 0 ? value : maxValue;
        }
        return maxValue;
    }


    /**
     * Returns the min value in the range for this task
     * @return  the min value in range
     */
    private Double maxDouble() {
        int count = 0;
        double maxValue = Double.MIN_VALUE;
        for (int i = from; i <= to; ++i) {
            if (!array.isNull(i)) {
                count++;
                final double value = array.getDouble(i);
                maxValue = Double.compare(value, maxValue) > 0 ? value : maxValue;
            }
        }
        return count == 0 ? null : maxValue;
    }


    /**
     * Returns the min value in the range for this task
     * @return  the min value in range
     */
    private T maxValue() {
        T maxValue = null;
        for (int i=0; i < array.length(); ++i) {
            if (!array.isNull(i)) {
                final T value = array.getValue(i);
                maxValue = maxValue == null ? value : comparator.compare(value, maxValue) > 0 ? value : maxValue;
            }
        }
        return maxValue;
    }


    /**
     * Returns the min value in the range for this task
     * @return  the min value in range
     */
    private T maxIntCoding() {
        int maxIndex = -1;
        int maxValue = Integer.MIN_VALUE;
        for (int i = from; i <= to; ++i) {
            if (!array.isNull(i)) {
                final int value = array.getInt(i);
                if (Integer.compare(value, maxValue) > 0) {
                    maxValue = value;
                    maxIndex = i;
                }
            }
        }
        return maxIndex < 0 ? null : array.getValue(maxIndex);
    }


    /**
     * Returns the min value in the range for this task
     * @return  the min value in range
     */
    private T maxLongCoding() {
        int maxIndex = -1;
        long maxValue = Long.MIN_VALUE;
        for (int i = from; i <= to; ++i) {
            if (!array.isNull(i)) {
                final long value = array.getLong(i);
                if (Long.compare(value, maxValue) > 0) {
                    maxValue = value;
                    maxIndex = i;
                }
            }
        }
        return maxIndex < 0 ? null : array.getValue(maxIndex);
    }

}
