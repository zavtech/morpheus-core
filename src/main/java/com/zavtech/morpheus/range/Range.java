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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.function.DoublePredicate;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBuilder;
import com.zavtech.morpheus.array.ArrayUtils;
import com.zavtech.morpheus.index.Index;

/**
 * An interface to a range of some type with a inclusive start and exclusive end
 *
 * @param <T>   the range element type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public interface Range<T> extends Iterable<T> {

    /**
     * Returns the start for this range
     * @return  the start for range, inclusive
     */
    T start();

    /**
     * Returns the end for this range
     * @return  the end for this range, exclusive
     */
    T end();

    /**
     * Returns an estimate of the size of this range
     * @return  an estimate of the range size
     */
    long estimateSize();

    /**
     * Returns true if this range is ascending, false if descending
     * @return  true if range is ascending, false if descending
     */
    boolean isAscending();

    /**
     * Splits this range into segments for parallel processing
     * @return  the list of segments, a singleton list if splitting not worth it
     */
    default List<Range<T>> split() {
        return Collections.singletonList(this);
    }

    /**
     * Splits this range into segments for parallel processing
     * @param splitThreshold    the number of intervals in the range required before splitting
     * @return  the list of segments, a singleton list if splitting not worth it
     */
    default List<Range<T>> split(int splitThreshold) {
        return Collections.singletonList(this);
    }

    /**
     * Returns a mapping of this range based on the mapper function provided
     * @param mapper    the mapper function
     * @param <R>       the mapped element type
     * @return          the mapped range
     */
    default <R> Range<R> map(Function<T,R> mapper) {
        return new RangeMapping<>(this, mapper);
    }

    /**
     * Returns a filtered view of this range based on the predicate specified
     * @param predicate     the predicate to filter range
     * @return              the filtered range
     */
    default Range<T> filter(Predicate<T> predicate) {
        return new RangeFilter<>(this, predicate);
    }

    /**
     * Returns a stream of values in this range
     * @return  the stream of values
     */
    default Stream<T> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED), false);
    }

    /**
     * Returns true if this range straddles the value specified
     * @param value     the value to check against
     * @return          true if this range straddles the value specified
     */
    @SuppressWarnings("unchecked")
    default boolean straddles(T value) {
        if (value instanceof Comparable) {
            final Comparable start = (Comparable)start();
            final Comparable end = (Comparable)end();
            final Comparable item = (Comparable)value;
            return item.compareTo(start) >= 0 && item.compareTo(end) <= 0;
        } else {
            throw new RuntimeException("Range.straddles() only supported for Comparable types, not " + start().getClass());
        }
    }

    /**
     * Returns an array of the elements in this range
     * @return      the array of elements in range
     */
    @SuppressWarnings("unchecked")
    default Array<T> toArray() {
        final long sizeEstimate = estimateSize();
        final Class<T> type = (Class<T>)start().getClass();
        if (type == Integer.class) {
            return toArray(sizeEstimate > 5000000);
        } else if (type == Long.class) {
            return toArray(sizeEstimate > 5000000);
        } else if (type == Double.class) {
            return toArray(sizeEstimate > 5000000);
        } else if (type == Date.class) {
            return toArray(sizeEstimate > 1000000);
        } else if (type == Instant.class) {
            return toArray(sizeEstimate > 1000000);
        } else if (type == LocalDate.class) {
            return toArray(sizeEstimate > 1000000);
        } else if (type == LocalTime.class) {
            return toArray(sizeEstimate > 1000000);
        } else if (type == LocalDateTime.class) {
            return toArray(sizeEstimate > 1000000);
        } else if (type == ZonedDateTime.class) {
            return toArray(sizeEstimate > 500000);
        } else {
            return toArray(sizeEstimate > 1000000);
        }
    }

    /**
     * Returns an array of the elements in this range
     * @param parallel  true to assemble the array using fork & join
     * @return          the array of elements in range
     */
    @SuppressWarnings("unchecked")
    default Array<T> toArray(boolean parallel) {
        if (!parallel) {
            final int length = (int)estimateSize();
            final Iterable<Object> iterable = (Iterable<Object>)this;
            return (Array<T>)ArrayBuilder.of(length).addAll(iterable).toArray();
        } else {
            final ToArrayTask<T> task = new ToArrayTask<>(this, 1000);
            return ForkJoinPool.commonPool().invoke(task);
        }
    }

    /**
     * Returns an array of the elements in this range
     * @param type  the desired array type
     * @return      the array of elements in range
     */
    @SuppressWarnings("unchecked")
    default Array<T> toArray(Class<T> type) {
        if (this instanceof RangeBase) {
            final int length = (int)((RangeBase)this).estimateSize();
            return stream().collect(ArrayUtils.toArray(type, length));
        } else {
            return stream().collect(ArrayUtils.toArray(type, 1000));
        }
    }

    /**
     * Returns an index of the elements in this range
     * @param type  the element type for the index
     * @return      the index of elements in range
     */
    default Index<T> toIndex(Class<T> type) {
        if (this instanceof RangeBase) {
            final int length = (int)((RangeBase)this).estimateSize();
            return Index.of(stream().collect(ArrayUtils.toArray(type, length)));
        } else {
            return Index.of(stream().collect(ArrayUtils.toArray(type, 1000)));
        }
    }


    /**
     * Returns a range based on the start and end specified
     * @param start the start date for range, inclusive
     * @param end   the end date for range, exclusive
     * @param <T>   the element type
     * @return      the resulting range
     * @throws IllegalArgumentException if type not supported
     */
    @SuppressWarnings("unchecked")
    static <T> Range<T> of(T start, T end) {
        if (start == null) {
            throw new IllegalArgumentException("The start of range cannot be null");
        } else if (end == null) {
            throw new IllegalArgumentException("The end of range cannot be null");
        } else if (start instanceof Integer) {
            return (Range<T>)Range.of(((Number)start).intValue(), ((Number)end).intValue());
        } else if (start instanceof Long) {
            return (Range<T>)Range.of(((Number)start).longValue(), ((Number)end).longValue());
        } else if (start instanceof Double) {
            return (Range<T>)Range.of(((Number)start).doubleValue(), ((Number)end).doubleValue());
        } else if (start instanceof LocalDate) {
            return (Range<T>)Range.of((LocalDate)start, (LocalDate)end);
        } else if (start instanceof LocalDateTime) {
            return (Range<T>)Range.of((LocalDateTime)start, (LocalDateTime)end, Duration.ofDays(1));
        } else if (start instanceof ZonedDateTime) {
            return (Range<T>)Range.of((ZonedDateTime) start, (ZonedDateTime)end, Duration.ofDays(1));
        } else if (start instanceof LocalTime) {
            return (Range<T>)Range.of((LocalTime) start, (LocalTime)end, Duration.ofMinutes(1));
        } else {
            throw new IllegalArgumentException("Unsupported type for range: " + start.getClass());
        }
    }

    /**
     * Returns a Range of ints between the start and end specified with step of 1
     * @param start the start of the range, inclusive
     * @param end   the end of the range, exclusive
     * @return      the newly created range
     */
    static Range<Integer> of(int start, int end) {
        return new RangeOfInts(start, end, 1, null);
    }

    /**
     * Returns a Range of ints between the start and end specified
     * @param start the start of the range, inclusive
     * @param end   the end of the range, exclusive
     * @param step  the step increment for range
     * @return      the newly created range
     */
    static Range<Integer> of(int start, int end, int step) {
        return new RangeOfInts(start, end, step, null);
    }

    /**
     * Returns a Range of ints between the start and end specified
     * @param start the start of the range, inclusive
     * @param end   the end of the range, exclusive
     * @param step  the step increment for range
     * @return      the newly created range
     */
    static Range<Integer> of(int start, int end, int step, IntPredicate excludes) {
        return new RangeOfInts(start, end, step, excludes);
    }

    /**
     * Returns a Range of longs between the start and end specified with step of 1
     * @param start the start of the range, inclusive
     * @param end   the end of the range, exclusive
     * @return      the newly created range
     */
    static Range<Long> of(long start, long end) {
        return new RangeOfLongs(start, end, 1, null);
    }

    /**
     * Returns a Range of longs between the start and end specified
     * @param start the start of the range, inclusive
     * @param end   the end of the range, exclusive
     * @param step  the step increment for range
     * @return      the newly created range
     */
    static Range<Long> of(long start, long end, long step) {
        return new RangeOfLongs(start, end, step, null);
    }

    /**
     * Returns a Range of longs between the start and end specified
     * @param start the start of the range, inclusive
     * @param end   the end of the range, exclusive
     * @param step  the step increment for range
     * @return      the newly created range
     */
    static Range<Long> of(long start, long end, long step, LongPredicate excludes) {
        return new RangeOfLongs(start, end, step, excludes);
    }

    /**
     * Returns a Range of longs between the start and end specified with step of 1
     * @param start the start of the range, inclusive
     * @param end   the end of the range, exclusive
     * @return      the newly created range
     */
    static Range<Double> of(double start, double end) {
        return new RangeOfDoubles(start, end, 1, null);
    }

    /**
     * Returns a Range of longs between the start and end specified
     * @param start the start of the range, inclusive
     * @param end   the end of the range, exclusive
     * @param step  the step increment for range
     * @return      the newly created range
     */
    static Range<Double> of(double start, double end, double step) {
        return new RangeOfDoubles(start, end, step, null);
    }

    /**
     * Returns a Range of longs between the start and end specified
     * @param start the start of the range, inclusive
     * @param end   the end of the range, exclusive
     * @param step  the step increment for range
     * @return      the newly created range
     */
    static Range<Double> of(double start, double end, double step, DoublePredicate excludes) {
        return new RangeOfDoubles(start, end, step, excludes);
    }

    /**
     * Returns a Range of LocalDate between the start and end specified with a step of 1-day
     * @param start the start of the range, inclusive
     * @param end   the end of the range, exclusive
     * @return      the newly created range
     */
    static Range<LocalDate> of(LocalDate start, LocalDate end) {
        return new RangeOfLocalDates(start, end, Period.ofDays(1), null);
    }

    /**
     * Returns a Range of LocalDate between the start and end specified
     * @param start the start of the range, inclusive
     * @param end   the end of the range, exclusive
     * @param step  the step increment for range
     * @return      the newly created range
     */
    static Range<LocalDate> of(LocalDate start, LocalDate end, Period step) {
        return new RangeOfLocalDates(start, end, step, null);
    }

    /**
     * Returns a Range of LocalDate between the start and end specified
     * @param start     the start of the range, inclusive
     * @param end       the end of the range, exclusive
     * @param step      the step increment for range
     * @param excludes  the predicate to exclude elements from the range, null permitted
     * @return          the newly created range
     */
    static Range<LocalDate> of(LocalDate start, LocalDate end, Period step, Predicate<LocalDate> excludes) {
        return new RangeOfLocalDates(start, end, step, excludes);
    }

    /**
     * Returns a Range of LocalTime between the start and end specified
     * @param start the start of the range, inclusive
     * @param end   the end of the range, exclusive
     * @param step  the step increment for range
     * @return      the newly created range
     */
    static Range<LocalTime> of(LocalTime start, LocalTime end, Duration step) {
        return new RangeOfLocalTimes(start, end, step, null);
    }

    /**
     * Returns a Range of LocalTime between the start and end specified
     * @param start     the start of the range, inclusive
     * @param end       the end of the range, exclusive
     * @param step      the step increment for range
     * @param excludes  the predicate to exclude elements from the range, null permitted
     * @return          the newly created range
     */
    static Range<LocalTime> of(LocalTime start, LocalTime end, Duration step, Predicate<LocalTime> excludes) {
        return new RangeOfLocalTimes(start, end, step, excludes);
    }

    /**
     * Returns a Range of LocalDateTime between the start and end specified
     * @param start the start of the range, inclusive
     * @param end   the end of the range, exclusive
     * @param step  the step increment for range
     * @return      the newly created range
     */
    static Range<LocalDateTime> of(LocalDateTime start, LocalDateTime end, Duration step) {
        return new RangeOfLocalDateTimes(start, end, step, null);
    }

    /**
     * Returns a Range of LocalDateTime between the start and end specified
     * @param start     the start of the range, inclusive
     * @param end       the end of the range, exclusive
     * @param step      the step increment for range
     * @param excludes  the predicate to exclude elements from the range, null permitted
     * @return          the newly created range
     */
    static Range<LocalDateTime> of(LocalDateTime start, LocalDateTime end, Duration step, Predicate<LocalDateTime> excludes) {
        return new RangeOfLocalDateTimes(start, end, step, excludes);
    }

    /**
     * Returns a Range of ZonedDateTime between the start and end specified
     * @param start the start of the range, inclusive
     * @param end   the end of the range, exclusive
     * @param step  the step increment for range
     * @return      the newly created range
     */
    static Range<ZonedDateTime> of(ZonedDateTime start, ZonedDateTime end, Duration step) {
        return new RangeOfZonedDateTimes(start, end, step, null);
    }

    /**
     * Returns a Range of ZonedDateTime between the start and end specified
     * @param start     the start of the range, inclusive
     * @param end       the end of the range, exclusive
     * @param step      the step increment for range
     * @param excludes  the predicate to exclude elements from the range, null permitted
     * @return          the newly created range
     */
    static Range<ZonedDateTime> of(ZonedDateTime start, ZonedDateTime end, Duration step, Predicate<ZonedDateTime> excludes) {
        return new RangeOfZonedDateTimes(start, end, step, excludes);
    }

    /**
     * Returns a LocalDate range based on the start and end date
     * @param start the start of range, inclusive, expressed as yyyy-MM-dd
     * @param end   the end of range, exclusive, expressed as yyyy-MM-dd
     * @return      the newly created range
     */
    static Range<LocalDate> ofLocalDates(String start, String end) {
        return Range.ofLocalDates(start, end, Period.ofDays(1));
    }

    /**
     * Returns a LocalDate range based on the start and end date
     * @param start the start of range, inclusive, expressed as yyyy-MM-dd
     * @param end   the end of range, exclusive, expressed as yyyy-MM-dd
     * @param step  the step increment for range
     * @return      the newly created range
     */
    static Range<LocalDate> ofLocalDates(String start, String end, Period step) {
        return Range.of(LocalDate.parse(start), LocalDate.parse(end), step);
    }


    /**
     * A Range implementation that maps some underlying range using a mapper function
     * @param <X>   the element type for underlying range
     * @param <Y>   the element type for mapped range
     */
    class RangeMapping<X,Y> extends RangeBase<Y> {

        private Range<X> range;
        private Function<X,Y> mapper;

        /**
         * Constructor
         * @param range     the range to apply
         * @param mapper  the mapping function
         */
        RangeMapping(Range<X> range, Function<X,Y> mapper) {
            super(null, null);
            this.range = range;
            this.mapper = mapper;
        }

        @Override
        public final Y start() {
            return mapper.apply(range.start());
        }

        @Override
        public final Y end() {
            return mapper.apply(range.end());
        }

        @Override
        public final long estimateSize() {
            return range.estimateSize();
        }

        @Override
        @SuppressWarnings("unchecked")
        public final boolean isAscending() {
            final Y start = mapper.apply(range.start());
            final Y end = mapper.apply(range.end());
            return ((Comparable)start).compareTo(end) <= 0;
        }

        @Override
        public final Iterator<Y> iterator() {
            final Iterator<X> source = range.iterator();
            return new Iterator<Y>() {
                @Override
                public boolean hasNext() {
                    return source.hasNext();
                }
                @Override
                public Y next() {
                    return mapper.apply(source.next());
                }
            };
        }
    }


    /**
     * A range filter that filters entries from some underlying range
     * @param <T>   the range element type
     */
    class RangeFilter<T> extends RangeBase<T> {

        private Range<T> underlying;
        private Predicate<T> predicate;

        /**
         * Constructor
         * @param underlying    the underlying range to operate on
         * @param predicate     the predicate
         */
        RangeFilter(Range<T> underlying, Predicate<T> predicate) {
            super(underlying.start(), underlying.end());
            this.underlying = underlying;
            this.predicate = predicate;
        }

        @Override
        public long estimateSize() {
            return underlying.estimateSize();
        }

        @Override
        public boolean isAscending() {
            return underlying.isAscending();
        }

        @Override
        public List<Range<T>> split() {
            return super.split().stream().map(r -> new RangeFilter<>(r, predicate)).collect(Collectors.toList());
        }

        @Override
        public List<Range<T>> split(int splitThreshold) {
            return super.split(splitThreshold).stream().map(r -> new RangeFilter<>(r, predicate)).collect(Collectors.toList());
        }

        @Override
        public Iterator<T> iterator() {
            final Iterator<T> source = underlying.iterator();
            return new Iterator<T>() {
                private T value;
                @Override
                public boolean hasNext() {
                    while (source.hasNext()) {
                        value = source.next();
                        if (predicate.test(value)) {
                            return true;
                        }
                    }
                    return false;
                }
                @Override
                public T next() {
                    return value;
                }
            };
        }
    }


    /**
     * A RecursiveTask that implements a parallel construction of an Array from a Range by splitting it into segments.
     */
    class ToArrayTask<T> extends RecursiveTask<Array<T>> {

        private final Range<T> range;
        private int splitThreshold;

        /**
         * Constructor
         * @param range the range to process
         * @param splitThreshold    the split threshold
         */
        ToArrayTask(Range<T> range, int splitThreshold) {
            this.range = range;
            this.splitThreshold = splitThreshold;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected Array<T> compute() {
            final List<Range<T>> segments = range.split(splitThreshold);
            if (segments.size() == 1) {
                final int sizeEstimate = (int)range.estimateSize();
                final Class<T> type = (Class<T>)range.start().getClass();
                return ArrayBuilder.of(sizeEstimate, type).addAll(range).toArray();
            } else {
                final Stream<ForkJoinTask<Array<T>>> stream = segments.stream().map(s -> new ToArrayTask<>(s, splitThreshold).fork());
                final List<ForkJoinTask<Array<T>>> tasks = stream.collect(Collectors.toList());
                final List<Array<T>> results = tasks.stream().map(ForkJoinTask::join).collect(Collectors.toList());
                final int multiplier = range.isAscending() ? 1 : -1;
                final Class<T> type = (Class<T>)range.start().getClass();
                Collections.sort(results, (s1, s2) -> ((Comparable)s1.getValue(0)).compareTo(s2.getValue(0)) * multiplier);
                return Array.concat(type, results);
            }
        }
    }



}
