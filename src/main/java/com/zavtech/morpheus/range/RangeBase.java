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
package com.zavtech.morpheus.range;

import java.util.Arrays;
import java.util.stream.IntStream;

/**
 * A convenience base class for building range implementations
 *
 * @param <T>   the element type for range
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
abstract class RangeBase<T> implements Range<T> {

    private T start;
    private T end;

    /**
     * Constructor
     * @param start     the start for range, inclusive
     * @param end       the end for range, exclusive
     */
    RangeBase(T start, T end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public T start() {
        return start;
    }

    @Override
    public T end() {
        return end;
    }

    protected final void setStart(T start) {
        this.start = start;
    }

    /**
     * Returns an array with more or less equal segment step counts as a resulting of splitting total steps into segments
     * The number of segments is determined by the machine processor count as per Runtime.getRuntime().availableProcessors()
     * @param totalStepCount    the total number of steps to segment into more or less equal parts
     * @return                  the array of segment step counts, which sum to the arg
     */
    int[] getSegmentSteps(int totalStepCount) {
        final int segmentStepCount = totalStepCount / Runtime.getRuntime().availableProcessors();
        final int segmentCount = totalStepCount / segmentStepCount;
        final int[] segmentSteps = new int[segmentCount];
        Arrays.fill(segmentSteps, segmentStepCount);
        final int padding = totalStepCount - IntStream.of(segmentSteps).sum();
        if (padding > 0) segmentSteps[segmentSteps.length-1] += padding;
        return segmentSteps;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Range)) return false;
        Range<?> other = (Range<?>) o;
        if (start != null ? !start.equals(other.start()) : other.start() != null) return false;
        return end != null ? end.equals(other.end()) : other.end() == null;

    }

    @Override
    public int hashCode() {
        int result = start() != null ? start().hashCode() : 0;
        result = 31 * result + (end() != null ? end().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "RangeBase{" + "start=" + start() + ", end=" + end() + '}';
    }
}
