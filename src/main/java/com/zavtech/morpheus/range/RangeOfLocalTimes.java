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

import java.time.Duration;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

/**
 * A Range implementation for LocalTimes
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class RangeOfLocalTimes extends RangeBase<LocalTime> {

    private Duration step;
    private boolean ascend;
    private Predicate<LocalTime> excludes;

    /**
     * Constructor
     * @param start     the start for range, inclusive
     * @param end       the end for range, exclusive
     * @param step      the step increment
     * @param excludes  optional predicate to exclude elements in this range
     */
    RangeOfLocalTimes(LocalTime start, LocalTime end, Duration step, Predicate<LocalTime> excludes) {
        super(start, end);
        this.step = step;
        this.ascend = start.compareTo(end) <= 0;
        this.excludes = excludes;
    }

    @Override
    public long estimateSize() {
        if (ascend) {
            final long stepSize = ChronoUnit.MILLIS.between(start(), start().plus(step));
            final long totalSize = ChronoUnit.MILLIS.between(start(), end());
            return (long)Math.ceil((double)totalSize / (double)stepSize);
        } else {
            final long stepSize = ChronoUnit.MILLIS.between(start(), start().plus(step));
            final long totalSize = ChronoUnit.MILLIS.between(end(), start());
            return (long)Math.ceil((double)totalSize / (double)stepSize);
        }
    }

    @Override
    public boolean isAscending() {
        return start().isBefore(end());
    }

    @Override
    public List<Range<LocalTime>> split() {
        return split(1000000);
    }

    @Override
    public List<Range<LocalTime>> split(int splitThreshold) {
        final int[] segmentSteps = getSegmentSteps((int)estimateSize());
        if (segmentSteps[0] < splitThreshold) {
            return Collections.singletonList(this);
        } else {
            final Duration stepSize = step;
            final List<Range<LocalTime>> segments = new ArrayList<>();
            for (int i=0; i<segmentSteps.length; ++i) {
                final Duration segmentSize = stepSize.multipliedBy(segmentSteps[i]);
                if (i == 0) {
                    final LocalTime end = isAscending() ? start().plus(segmentSize) : start().minus(segmentSize);
                    final Range<LocalTime> range = Range.of(start(), end, step, excludes);
                    segments.add(range);
                } else {
                    final Range<LocalTime> previous = segments.get(i-1);
                    final LocalTime end = isAscending() ? previous.end().plus(segmentSize) : previous.end().minus(segmentSize);
                    final Range<LocalTime> next = Range.of(previous.end(), end, step, excludes);
                    segments.add(next);
                }
            }
            return segments;
        }
    }

    @Override
    public final Iterator<LocalTime> iterator() {
        return new Iterator<LocalTime>() {
            private LocalTime value = start();
            @Override
            public boolean hasNext() {
                if (excludes != null) {
                    while (excludes.test(value) && inBounds(value)) {
                        value = ascend ? value.plus(step) : value.minus(step);
                    }
                }
                return inBounds(value);
            }
            @Override
            public LocalTime next() {
                final LocalTime next = value;
                value = ascend ? value.plus(step) : value.minus(step);
                return next;
            }
        };
    }

    /**
     * Checks that the value specified is in the bounds of this range
     * @param value     the value to check if in bounds
     * @return          true if in bounds
     */
    private boolean inBounds(LocalTime value) {
        return ascend ? value.compareTo(start()) >=0 && value.isBefore(end()) : value.compareTo(start()) <=0 && value.isAfter(end());
    }
}
