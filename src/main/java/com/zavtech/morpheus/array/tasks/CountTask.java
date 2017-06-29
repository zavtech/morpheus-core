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

import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.function.Predicate;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayCursor;
import com.zavtech.morpheus.array.ArrayException;
import com.zavtech.morpheus.array.ArrayValue;

/**
 * A RecursiveTask implementation that yields the number of values that match a predicate in a Morpheus Array
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class CountTask<T> extends RecursiveTask<Integer> {

    private Array<T> array;
    private int from, to, splitThreshold;
    private Predicate<ArrayValue<T>> predicate;

    /**
     * Constructor
     * @param array             the array to operate on
     * @param from              the from index in array, inclusive
     * @param to                the to index in array, inclusive
     * @param splitThreshold    the split threshold, below which not to split further
     * @param predicate         the predicate for this task
     */
    @SuppressWarnings("unchecked")
    public CountTask(Array<T> array, int from, int to, int splitThreshold, Predicate<ArrayValue<T>> predicate) {
        this.array = array;
        this.from = from;
        this.to = to;
        this.splitThreshold = splitThreshold;
        this.predicate = predicate;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Integer compute() {
        try {
            if (array.length() == 0) return 0;
            final int length = to - from + 1;
            if (length > splitThreshold) {
                final int splitLength = (to - from) / 2;
                final int midPoint = from + splitLength;
                final ForkJoinTask<Integer> left = new CountTask(array, from, midPoint, splitThreshold, predicate).fork();
                final CountTask right = new CountTask(array, midPoint + 1, to, splitThreshold, predicate);
                final Integer x = right.compute();
                final Integer y  = left.join();
                return x + y;
            } else {
                int count = 0;
                final ArrayCursor value = array.cursor();
                for (int i = from; i <= to; ++i) {
                    value.moveTo(i);
                    if (predicate.test(value)) {
                        count++;
                    }
                }
                return count;
            }
        } catch (ArrayException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ArrayException("Failed to compute count for predicate", ex);
        }
    }

}