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
import java.util.function.LongPredicate;

/**
 * A Range implementation for Longs
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class RangeOfLongs extends RangeBase<Long> {

    private long start;
    private long end;
    private long step;
    private boolean ascend;
    private LongPredicate excludes;

    /**
     * Constructor
     * @param start     the start for range, inclusive
     * @param end       the end for range, exclusive
     * @param step      the step increment
     * @param excludes  optional predicate to exclude elements in this range
     */
    RangeOfLongs(long start, long end, long step, LongPredicate excludes) {
        super(start, end);
        this.start = start;
        this.end = end;
        this.step = step;
        this.ascend = start <= end;
        this.excludes = excludes;
    }

    @Override()
    public final long estimateSize() {
        return (long)Math.ceil((double)Math.abs(end-start) / (double)step);
    }

    @Override
    public boolean isAscending() {
        return start < end;
    }

    @Override
    public List<Range<Long>> split() {
        return split(1000000);
    }

    @Override
    public List<Range<Long>> split(int splitThreshold) {
        final int[] segmentSteps = getSegmentSteps((int)estimateSize());
        if (segmentSteps[0] < splitThreshold) {
            return Collections.singletonList(this);
        } else {
            final long stepSize = step;
            final List<Range<Long>> segments = new ArrayList<>();
            for (int i=0; i<segmentSteps.length; ++i) {
                final long segmentSize = stepSize * segmentSteps[i];
                if (i == 0) {
                    final long end = start() + segmentSize * (isAscending() ? 1 : -1);
                    final Range<Long> range = Range.of(start(), end, step, excludes);
                    segments.add(range);
                } else {
                    final Range<Long> previous = segments.get(i-1);
                    final long end = previous.end() + segmentSize * (isAscending() ? 1 : -1);
                    final Range<Long> next = Range.of(previous.end(), end, step, excludes);
                    segments.add(next);
                }
            }
            return segments;
        }
    }

    @Override
    public final Iterator<Long> iterator() {
        return iteratorOfLong();
    }

    /**
     * Checks that the value specified is in the bounds of this range
     * @param value     the value to check if in bounds
     * @return          true if in bounds
     */
    private boolean inBounds(long value) {
        return ascend ? value >= start && value < end : value <= start && value > end;
    }

    /**
     * Returns a primitive iterator for this range
     * @return  the primitive iterator
     */
    private PrimitiveIterator.OfLong iteratorOfLong() {
        return new PrimitiveIterator.OfLong() {
            private long value = start;
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
            public long nextLong() {
                final long next = value;
                value = ascend ? value + step : value - step;
                return next;
            }
        };
    }
}
