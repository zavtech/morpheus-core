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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.function.DoublePredicate;

/**
 * A Range implementation for Doubles
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class RangeOfDoubles extends RangeBase<Double> {

    private static final double EPSILON = 10e-12;

    private double start;
    private double end;
    private double step;
    private boolean ascend;
    private DoublePredicate excludes;

    /**
     * Constructor
     * @param start     the start for range, inclusive
     * @param end       the end for range, exclusive
     * @param step      the step increment
     * @param excludes  optional predicate to exclude elements in this range
     */
    RangeOfDoubles(double start, double end, double step, DoublePredicate excludes) {
        super(start, end);
        this.start = start;
        this.end = end;
        this.step = step;
        this.ascend = start <= end;
        this.excludes = excludes;
    }

    @Override()
    public final long estimateSize() {
        return (long)Math.ceil(Math.abs(end-start) / step);
    }

    @Override
    public boolean isAscending() {
        return start < end;
    }

    @Override
    public List<Range<Double>> split() {
        return split(1000000);
    }

    @Override
    public List<Range<Double>> split(int splitThreshold) {
        final int[] segmentSteps = getSegmentSteps((int)estimateSize());
        if (segmentSteps[0] < splitThreshold) {
            return Collections.singletonList(this);
        } else {
            final double stepSize = step;
            final List<Range<Double>> segments = new ArrayList<>();
            for (int i=0; i<segmentSteps.length; ++i) {
                final double segmentSize = stepSize * segmentSteps[i];
                if (i == 0) {
                    final double end = start() + segmentSize * (isAscending() ? 1 : -1);
                    final Range<Double> range = Range.of(start(), end, step, excludes);
                    segments.add(range);
                } else {
                    final Range<Double> previous = segments.get(i-1);
                    final double end = previous.end() + segmentSize * (isAscending() ? 1 : -1);
                    final Range<Double> next = Range.of(previous.end(), end, step, excludes);
                    segments.add(next);
                }
            }
            return segments;
        }
    }

    @Override
    public final Iterator<Double> iterator() {
        return iteratorOfDouble();
    }

    /**
     * Checks that the value specified is in the bounds of this range
     * @param value     the value to check if in bounds
     * @return          true if in bounds
     */
    private boolean inBounds(double value) {
        return ascend ? value >= start && value < end - EPSILON : value <= start && value > end + EPSILON;
    }

    /**
     * Returns a primitive iterator for this range
     * @return  the primitive iterator
     */
    private PrimitiveIterator.OfDouble iteratorOfDouble() {
        return new PrimitiveIterator.OfDouble() {
            private double value = start;
            @Override
            public boolean hasNext() {
                if (excludes != null) {
                    while (excludes.test(value) && inBounds(value)) {
                        value = ascend ? value + step : value - step;
                    }
                }
                return inBounds(value);
            }
            @Override
            public double nextDouble() {
                final double next = value;
                value = ascend ? value + step : value - step;
                return next;
            }
        };
    }
}
