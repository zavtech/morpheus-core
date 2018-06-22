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
package com.zavtech.morpheus.array;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.AbstractList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.zavtech.morpheus.array.tasks.BoundsTask;
import com.zavtech.morpheus.array.tasks.CountTask;
import com.zavtech.morpheus.array.tasks.MaxTask;
import com.zavtech.morpheus.array.tasks.MinTask;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.stats.Stats;
import com.zavtech.morpheus.util.functions.BooleanConsumer;
import com.zavtech.morpheus.util.Bounds;
import com.zavtech.morpheus.util.Comparators;
import com.zavtech.morpheus.util.IntComparator;
import com.zavtech.morpheus.util.SortAlgorithm;
import com.zavtech.morpheus.util.functions.ToBooleanFunction;

import it.unimi.dsi.fastutil.Arrays;

/**
 * A convenience base class used to build Morpheus Array implementations
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public abstract class ArrayBase<T> implements Array<T> {

    private static final long serialVersionUID = 1L;

    private Class<T> type;
    private boolean parallel;
    private ArrayStyle style;

    /**
     * Constructor
     * @param type      the array type
     * @param style     the array style
     * @param parallel  true for parallel version
     */
    protected ArrayBase(Class<T> type, ArrayStyle style, boolean parallel) {
        this.type = type;
        this.style = style;
        this.parallel = parallel;
    }

    /**
     * Checks the index is in bounds for this array
     * @param index     the array index
     * @param length    the array length
     */
    protected final void checkBounds(int index, int length) {
        if (index > length || index < 0) {
            throw new ArrayIndexOutOfBoundsException("Array index out of bounds: " + index + ", length " + length);
        }
    }

    @Override
    public final Class<T> type() {
        return type;
    }

    @Override
    public final ArrayStyle style() {
        return style;
    }

    @Override
    public final ArrayType typeCode() {
        return ArrayType.of(type);
    }

    @Override
    public final boolean isParallel() {
        return parallel;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public Array<T> readOnly() {
        return isReadOnly() ? this : new ArrayReadOnly<>(this);
    }

    @Override
    public final ArrayCursor<T> cursor() {
        return new ArrayValueCursor();
    }


    /**
     * Performs a quick sort over the range specified
     * @param start         the start index for range
     * @param end           the end index for range
     * @param comparator    the comparator
     * @return              this array
     */
    protected Array<T> doSort(int start, int end, IntComparator comparator) {
        SortAlgorithm.getDefault(parallel).sort(start, end, comparator, this::swap);
        return this;
    }


    /**
     * Sorts this array in the range specified, applying the multiplier to default comparator result
     * @param start         the start index in sort range, inclusive
     * @param end           the end index in sort range, exclusive
     * @param multiplier    the multiplier to apply to comparator result
     * @return              this array reference
     */
    @SuppressWarnings("unchecked")
    protected Array<T> sort(int start, int end, int multiplier) {
        final Class<T> type = type();
        final Comparator<T> comparator = Comparators.getDefaultComparator(type);
        return doSort(start, end, (int i, int j) -> {
            final T o1 = getValue(i);
            final T o2 = getValue(j);
            return multiplier * comparator.compare(o1, o2);
        });
    }


    @Override
    public final Array<T> concat(Array<T> other) {
        final Class<T> clazz = type();
        final Array<T> result = Array.of(clazz, length() + other.length());
        final int[] indexes1 = IntStream.range(0, length()).toArray();
        final int[] indexes2 = IntStream.range(0, other.length()).toArray();
        final int[] indexes3 = IntStream.range(0, other.length()).map(i -> i+this.length()).toArray();
        result.update(this, indexes1, indexes1);
        result.update(other, indexes2, indexes3);
        return result;
    }


    @Override()
    public Array<T> fill(T value) {
        return fill(value, 0, length());
    }


    @Override()
    public Array<T> distinct() {
        return distinct(Arrays.MAX_ARRAY_SIZE);
    }


    @Override()
    @SuppressWarnings("unchecked")
    public Array<T> distinct(int limit) {
        switch(typeCode()) {
            case INTEGER:   return (Array<T>)ArrayUtils.distinct(stream().ints(), limit);
            case LONG:      return (Array<T>)ArrayUtils.distinct(stream().longs(), limit);
            case DOUBLE:    return (Array<T>)ArrayUtils.distinct(stream().doubles(), limit);
            default:
                final int capacity = limit < it.unimi.dsi.fastutil.Arrays.MAX_ARRAY_SIZE ? limit : 1000;
                final Set<T> set = new HashSet<>(capacity);
                final ArrayBuilder<T> builder = ArrayBuilder.of(capacity, type());
                for (int i=0; i<length(); ++i) {
                    final T value = getValue(i);
                    if (set.add(value)) {
                        builder.add(value);
                        if (set.size() >= limit) {
                            break;
                        }
                    }
                }
                return builder.toArray();
        }
    }


    @Override()
    public Array<T> cumSum() {
        throw new ArrayException("Cumulative sum is only supported by numeric Array types, not " + type());
    }



    @Override
    public final Array<Boolean> mapToBooleans(ToBooleanFunction<ArrayValue<T>> mapper) {
        final Array<Boolean> result = Array.of(Boolean.class, length());
        final MapValues<Boolean> action = new MapValues<>(0, length() - 1, mapper, result);
        if (isParallel()) {
            ForkJoinPool.commonPool().invoke(action);
            return result;
        } else {
            action.compute();
            return result;
        }
    }


    @Override
    public final Array<Integer> mapToInts(ToIntFunction<ArrayValue<T>> mapper) {
        final Array<Integer> result = Array.of(Integer.class, length());
        final MapValues<Integer> action = new MapValues<>(0, length() - 1, mapper, result);
        if (isParallel()) {
            ForkJoinPool.commonPool().invoke(action);
            return result;
        } else {
            action.compute();
            return result;
        }
    }


    @Override
    public final Array<Long> mapToLongs(ToLongFunction<ArrayValue<T>> mapper) {
        final Array<Long> result = Array.of(Long.class, length());
        final MapValues<Long> action = new MapValues<>(0, length() - 1, mapper, result);
        if (isParallel()) {
            ForkJoinPool.commonPool().invoke(action);
            return result;
        } else {
            action.compute();
            return result;
        }
    }


    @Override
    public final Array<Double> mapToDoubles(ToDoubleFunction<ArrayValue<T>> mapper) {
        final Array<Double> result = Array.of(Double.class, length());
        final MapValues<Double> action = new MapValues<>(0, length() - 1, mapper, result);
        if (isParallel()) {
            ForkJoinPool.commonPool().invoke(action);
            return result;
        } else {
            action.compute();
            return result;
        }
    }


    @Override
    public final <R> Array<R> map(Function<ArrayValue<T>,R> mapper) {
        final ArrayBuilder<R> builder = ArrayBuilder.of(length());
        this.sequential().forEachValue(v -> builder.add(mapper.apply(v)));
        return builder.toArray();
    }


    @Override
    public final Array<T> applyBooleans(ToBooleanFunction<ArrayValue<T>> function) {
        final int length = length();
        if (length > 0) {
            final ApplyValues action = new ApplyValues(0, length - 1, function);
            if (isParallel()) {
                ForkJoinPool.commonPool().invoke(action);
            } else {
                action.compute();
            }
        }
        return this;
    }


    @Override
    public final Array<T> applyInts(ToIntFunction<ArrayValue<T>> function) {
        final int length = length();
        if (length > 0) {
            final ApplyValues action = new ApplyValues(0, length - 1, function);
            if (isParallel()) {
                ForkJoinPool.commonPool().invoke(action);
            } else {
                action.compute();
            }
        }
        return this;
    }


    @Override
    public final Array<T> applyLongs(ToLongFunction<ArrayValue<T>> function) {
        final int length = length();
        if (length > 0) {
            final ApplyValues action = new ApplyValues(0, length - 1, function);
            if (isParallel()) {
                ForkJoinPool.commonPool().invoke(action);
            } else {
                action.compute();
            }
        }
        return this;
    }


    @Override
    public final Array<T> applyDoubles(ToDoubleFunction<ArrayValue<T>> function) {
        final int length = length();
        if (length > 0) {
            final ApplyValues action = new ApplyValues(0, length - 1, function);
            if (isParallel()) {
                ForkJoinPool.commonPool().invoke(action);
            } else {
                action.compute();
            }
        }
        return this;
    }


    @Override
    public final Array<T> applyValues(Function<ArrayValue<T>,T> function) {
        final int length = length();
        if (length > 0) {
            final ApplyValues action = new ApplyValues(0, length - 1, function);
            if (isParallel()) {
                ForkJoinPool.commonPool().invoke(action);
            } else {
                action.compute();
            }
        }
        return this;
    }


    @Override
    public final void forEach(Consumer<? super T> consumer) {
        final int length = length();
        if (isParallel() && length > 0) {
            final int processors = Runtime.getRuntime().availableProcessors();
            final int splitThreshold = parallel ? Math.max(length() / processors, 10000) : Integer.MAX_VALUE;
            final ForEach action = new ForEach(0, length - 1, splitThreshold, consumer);
            ForkJoinPool.commonPool().invoke(action);
        } else {
            for (int i=0; i<length; ++i) {
                final T value = getValue(i);
                consumer.accept(value);
            }
        }
    }

    @Override
    public Array<T> forEachBoolean(BooleanConsumer consumer) {
        final int length = length();
        if (isParallel() && length > 0) {
            final int processors = Runtime.getRuntime().availableProcessors();
            final int splitThreshold = parallel ? Math.max(length() / processors, 10000) : Integer.MAX_VALUE;
            final ForEach action = new ForEach(0, length - 1, splitThreshold, consumer);
            ForkJoinPool.commonPool().invoke(action);
        } else {
            for (int i=0; i<length; ++i) {
                final boolean value = getBoolean(i);
                consumer.accept(value);
            }
        }
        return this;
    }

    @Override
    public Array<T> forEachInt(IntConsumer consumer) {
        final int length = length();
        if (isParallel() && length > 0) {
            final int processors = Runtime.getRuntime().availableProcessors();
            final int splitThreshold = parallel ? Math.max(length() / processors, 10000) : Integer.MAX_VALUE;
            final ForEach action = new ForEach(0, length - 1, splitThreshold, consumer);
            ForkJoinPool.commonPool().invoke(action);
        } else {
            for (int i=0; i<length; ++i) {
                final int value = getInt(i);
                consumer.accept(value);
            }
        }
        return this;
    }

    @Override
    public Array<T> forEachLong(LongConsumer consumer) {
        final int length = length();
        if (isParallel() && length > 0) {
            final int processors = Runtime.getRuntime().availableProcessors();
            final int splitThreshold = parallel ? Math.max(length() / processors, 10000) : Integer.MAX_VALUE;
            final ForEach action = new ForEach(0, length - 1, splitThreshold, consumer);
            ForkJoinPool.commonPool().invoke(action);
        } else {
            for (int i=0; i<length; ++i) {
                final long value = getLong(i);
                consumer.accept(value);
            }
        }
        return this;
    }

    @Override
    public final Array<T> forEachDouble(DoubleConsumer consumer) {
        final int length = length();
        if (isParallel() && length > 0) {
            final int processors = Runtime.getRuntime().availableProcessors();
            final int splitThreshold = parallel ? Math.max(length() / processors, 10000) : Integer.MAX_VALUE;
            final ForEach action = new ForEach(0, length - 1, splitThreshold, consumer);
            ForkJoinPool.commonPool().invoke(action);
        } else {
            for (int i=0; i<length; ++i) {
                final double value = getDouble(i);
                consumer.accept(value);
            }
        }
        return this;
    }

    @Override
    public final Array<T> forEachValue(Consumer<ArrayValue<T>> consumer) {
        final int length = length();
        if (isParallel() && length > 0) {
            final int processors = Runtime.getRuntime().availableProcessors();
            final int splitThreshold = parallel ? Math.max(length() / processors, 10000) : Integer.MAX_VALUE;
            final ForEachArrayValue action = new ForEachArrayValue(0, length - 1, splitThreshold, consumer);
            ForkJoinPool.commonPool().invoke(action);
        } else {
            final ForEachArrayValue action = new ForEachArrayValue(0, length - 1, Integer.MAX_VALUE, consumer);
            action.compute();
        }
        return this;
    }


    @Override
    public final Optional<ArrayValue<T>> previous(T value) {
        final int length = length();
        if (length == 0) {
            return Optional.empty();
        } else {
            final int insertionPoint = binarySearch(0, length, value);
            final int index = insertionPoint < 0 ? (insertionPoint * -1) - 2 : insertionPoint - 1;
            if (index >= 0 && index < length) {
                final ArrayValue<T> result = cursor().moveTo(index);
                return Optional.ofNullable(result);
            } else {
                return Optional.empty();
            }
        }
    }


    @Override
    public final Optional<ArrayValue<T>> next(T value) {
        final int length = length();
        if (length == 0) {
            return Optional.empty();
        } else {
            final int insertionPoint = binarySearch(0, length, value);
            final int index = insertionPoint < 0 ? (insertionPoint * -1) - 1 : insertionPoint + 1;
            if (index >= 0 && index < length) {
                final ArrayValue<T> result = cursor().moveTo(index);
                return Optional.ofNullable(result);
            } else {
                return Optional.empty();
            }
        }
    }


    @Override
    public Array<T> sort(boolean ascending) {
        return sort(0, length(), ascending ? 1 : -1);
    }


    @Override
    public Array<T> sort(int start, int end, boolean ascending) {
        return sort(start, end, ascending ? 1 : -1);
    }


    @Override
    public Array<T> sort(int start, int end, Comparator<ArrayValue<T>> comparator) {
        return doSort(start, end, new ArrayIntComparator(comparator));
    }


    @Override
    public final Optional<ArrayValue<T>> first(Predicate<ArrayValue<T>> predicate) {
        if (length() == 0) {
            return Optional.empty();
        } else {
            final int length = this.length();
            final ArrayCursor<T> cursor = cursor();
            for (int i=0; i<length; ++i) {
                cursor.moveTo(i);
                if (predicate.test(cursor)) {
                    return Optional.of(cursor);
                }
            }
            return Optional.empty();
        }
    }


    @Override
    public final Optional<ArrayValue<T>> last(Predicate<ArrayValue<T>> predicate) {
        if (length() == 0) {
            return Optional.empty();
        } else {
            final int length = this.length();
            final ArrayCursor<T> cursor = cursor();
            for (int i=length-1; i>=0; --i) {
                cursor.moveTo(i);
                if (predicate.test(cursor)) {
                    return Optional.of(cursor);
                }
            }
            return Optional.empty();
        }
    }


    @Override
    public final Optional<T> min() {
        if (isParallel() && length() > 0) {
            final int processors = Runtime.getRuntime().availableProcessors();
            final int splitThreshold = Math.max(length() / processors, 10000);
            final MinTask<T> task = new MinTask<>(this, 0, length()-1, splitThreshold);
            final T minValue = ForkJoinPool.commonPool().invoke(task);
            return Optional.ofNullable(minValue);
        } else {
            final MinTask<T> task = new MinTask<>(this, 0, length()-1, Integer.MAX_VALUE);
            final T minValue = task.compute();
            return Optional.ofNullable(minValue);
        }
    }


    @Override
    public final Optional<T> max() {
        if (isParallel() && length() > 0) {
            final int processors = Runtime.getRuntime().availableProcessors();
            final int splitThreshold = Math.max(length() / processors, 10000);
            final MaxTask<T> task = new MaxTask<>(this, 0, length()-1, splitThreshold);
            final T maxValue = ForkJoinPool.commonPool().invoke(task);
            return Optional.ofNullable(maxValue);
        } else {
            final MaxTask<T> task = new MaxTask<>(this, 0, length()-1, Integer.MAX_VALUE);
            final T maxValue = task.compute();
            return Optional.ofNullable(maxValue);
        }
    }


    @Override()
    public final Optional<Bounds<T>> bounds() {
        if (isParallel() && length() > 0) {
            final int processors = Runtime.getRuntime().availableProcessors();
            final int splitThreshold = Math.max(length() / processors, 10000);
            final BoundsTask<T> task = new BoundsTask<>(this, 0, length()-1, splitThreshold);
            final Bounds<T> bounds = ForkJoinPool.commonPool().invoke(task);
            return Optional.ofNullable(bounds);
        } else {
            final BoundsTask<T> task = new BoundsTask<>(this, 0, length()-1, Integer.MAX_VALUE);
            final Bounds<T> bounds = task.compute();
            return Optional.ofNullable(bounds);
        }
    }


    @Override()
    @SuppressWarnings("unchecked")
    public Stats<Number> stats() throws ArrayException {
        switch (typeCode()) {
            case INTEGER:       return new ArrayStats<>((Array<Number>)this, 0, length());
            case LONG:          return new ArrayStats<>((Array<Number>)this, 0, length());
            case DOUBLE:        return new ArrayStats<>((Array<Number>)this, 0, length());
            default:    throw new IllegalStateException("The array is non-numeric: " + typeCode());
        }
    }


    @Override()
    @SuppressWarnings("unchecked")
    public Stats<Number> stats(int offset, int length) {
        switch (typeCode()) {
            case INTEGER:       return new ArrayStats<>((Array<Number>)this, offset, length);
            case LONG:          return new ArrayStats<>((Array<Number>)this, offset, length);
            case DOUBLE:        return new ArrayStats<>((Array<Number>)this, offset, length);
            default:    throw new IllegalStateException("The array is non-numeric: " + typeCode());
        }
    }


    @Override
    public final List<T> toList() {
        return new ListWrapper<>(this);
    }


    @Override()
    public final ArrayStreams<T> stream() {
        return new Streams<>(this, 0, length());
    }


    @Override()
    public final ArrayStreams<T> stream(int start, int end) {
        this.checkBounds(start, length());
        this.checkBounds(end, length());
        return new Streams<>(this, start, end);
    }


    @Override()
    public Array<T> shuffle(int count) {
        final Random random = ThreadLocalRandom.current();
        final int length = length();
        for (int i=0; i<count; ++i) {
            for (int j=0; j<length; ++j) {
                this.swap(j, random.nextInt(length));
            }
        }
        return this;
    }


    @Override
    public final int binarySearch(T value) {
        return binarySearch(0, length(), value);
    }


    @Override
    @SuppressWarnings("unchecked")
    public int binarySearch(int start, int end, T value) {
        try {
            int low = start;
            int high = end - 1;
            final Class<T> type = type();
            final Comparator<T> comparator = Comparators.getDefaultComparator(type);
            while (low <= high) {
                final int midIndex = (low + high) >>> 1;
                final T midValue = getValue(midIndex);
                final int result = comparator.compare(midValue, value);
                if (result < 0) {
                    low = midIndex + 1;
                } else if (result > 0) {
                    high = midIndex - 1;
                } else {
                    return midIndex;
                }
            }
            return -(low + 1);
        } catch (Exception ex) {
            throw new ArrayException("Binary search of array failed", ex);
        }
    }


    @Override
    public int count(Predicate<ArrayValue<T>> predicate) {
        if (isParallel() && length() > 0) {
            final int processors = Runtime.getRuntime().availableProcessors();
            final int splitThreshold = Math.max(length() / processors, 10000);
            return ForkJoinPool.commonPool().invoke(new CountTask<>(this, 0, length()-1, splitThreshold, predicate));
        } else {
            final CountTask task = new CountTask<>(this, 0, length()-1, Integer.MAX_VALUE, predicate);
            return task.compute();
        }
    }


    @Override
    public boolean getBoolean(int index) {
        throw new ArrayException("Boolean type not supported by this array, type = " + typeCode().name());
    }


    @Override
    public int getInt(int index) {
        throw new ArrayException("Int type not supported by this array, type = " + typeCode().name());
    }


    @Override
    public long getLong(int index) {
        throw new ArrayException("Long type not supported by this array, type = " + typeCode().name());
    }


    @Override
    public double getDouble(int index) {
        throw new ArrayException("Double type not supported by this array, type = " + typeCode().name());
    }


    @Override
    public boolean setBoolean(int index, boolean value) {
        throw new ArrayException("Boolean type not supported by this array, type = " + typeCode().name());
    }


    @Override
    public int setInt(int index, int value) {
        throw new ArrayException("Int type not supported by this array, type = " + typeCode().name());
    }


    @Override
    public long setLong(int index, long value) {
        throw new ArrayException("Long type not supported by this array, type = " + typeCode().name());
    }


    @Override
    public double setDouble(int index, double value) {
        throw new ArrayException("Double type not supported by this array, type = " + typeCode().name());
    }


    @Override()
    public final Iterator<T> iterator() {
        return new Iterator<T>() {
            private int index = -1;
            @Override
            public boolean hasNext() {
                return ++index < length();
            }
            @Override
            public T next() {
                return getValue(index);
            }
        };
    }


    @Override()
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        } else if (other == this) {
            return true;
        } else if (!(other instanceof Array)) {
            return false;
        } else {
            final Array array = (Array)other;
            if (!array.typeCode().equals(this.typeCode()) || array.length() != this.length()) {
                return false;
            } else {
                switch (typeCode()) {
                    case BOOLEAN:           return booleanEquals(array);
                    case INTEGER:           return intEquals(array);
                    case LONG:              return longEquals(array);
                    case DOUBLE:            return doubleEquals(array);
                    case DATE:              return longEquals(array);
                    case ENUM:              return intEquals(array);
                    case ZONE_ID:           return intEquals(array);
                    case TIME_ZONE:         return intEquals(array);
                    case LOCAL_DATE:        return longEquals(array);
                    case LOCAL_TIME:        return longEquals(array);
                    case LOCAL_DATETIME:    return longEquals(array);
                    case ZONED_DATETIME:    return longEquals(array);
                    default:                return objectEquals(array);
                }
            }
        }
    }


    /**
     * Returns true if two boolean arrays are equal
     * @param array the array to compare with
     * @return      true if all elements are equal
     */
    private boolean booleanEquals(Array<?> array) {
        final int length = array.length();
        for (int i=0; i<length; ++i) {
            final boolean v1 = array.getBoolean(i);
            final boolean v2 = this.getBoolean(i);
            if (v1 != v2) {
                return false;
            }
        }
        return true;
    }


    /**
     * Returns true if two integer arrays are equal
     * @param array the array to compare with
     * @return      true if all elements are equal
     */
    private boolean intEquals(Array<?> array) {
        final int length = array.length();
        for (int i=0; i<length; ++i) {
            final int v1 = array.getInt(i);
            final int v2 = this.getInt(i);
            if (v1 != v2) {
                return false;
            }
        }
        return true;
    }


    /**
     * Returns true if two long arrays are equal
     * @param array the array to compare with
     * @return      true if all elements are equal
     */
    private boolean longEquals(Array<?> array) {
        final int length = array.length();
        for (int i=0; i<length; ++i) {
            final long v1 = array.getLong(i);
            final long v2 = this.getLong(i);
            if (v1 != v2) {
                return false;
            }
        }
        return true;
    }


    /**
     * Returns true if two double arrays are equal
     * @param array the array to compare with
     * @return      true if all elements are equal
     */
    private boolean doubleEquals(Array<?> array) {
        final int length = array.length();
        for (int i=0; i<length; ++i) {
            final double v1 = array.getDouble(i);
            final double v2 = this.getDouble(i);
            if (Double.doubleToLongBits(v1) != Double.doubleToLongBits(v2)) {
                return false;
            }
        }
        return true;
    }


    /**
     * Returns true if two arrays are equal
     * @param array the array to compare with
     * @return      true if all elements are equal
     */
    private boolean objectEquals(Array<?> array) {
        final int length = array.length();
        for (int i=0; i<length; ++i) {
            final Object v1 = array.getValue(i);
            final Object v2 = this.getValue(i);
            if (!(v1==null ? v2==null : v1.equals(v2))) {
                return false;
            }
        }
        return true;
    }


    /** Custom serialization */
    private void writeObject(ObjectOutputStream os) throws IOException {
        os.writeObject(type);
        os.writeUTF(style.name());
        os.writeBoolean(parallel);
    }


    @SuppressWarnings("unchecked")
    /** Custom serialization */
    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
        this.type = (Class<T>)is.readObject();
        this.style = ArrayStyle.valueOf(is.readUTF());
        this.parallel = is.readBoolean();
    }


    @Override()
    public String toString() {
        return "Array type=" + typeCode().name()
                + ", length=" + length()
                + ", defaultValue=" + defaultValue()
                + ", start=" + first(v -> true).map(Object::toString).orElse("N/A")
                + ", end=" + last(v -> true).map(Object::toString).orElse("N/A");
    }




    /**
     * A RecursiveAction to apply values to each element in this Array
     */
    private class ApplyValues extends RecursiveAction {

        private int from, to;
        private Object function;

        /**
         * Constructor
         * @param from      from ordinal
         * @param to        to ordinal
         * @param function  the consumer
         */
        @SuppressWarnings("unchecked")
        ApplyValues(int from, int to, Object function) {
            this.from = from;
            this.to = to;
            this.function = function;
            if (from > to) {
                throw new DataFrameException("The to index must be > from index");
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        protected void compute() {
            try {
                final int count = to - from + 1;
                final int processors = Runtime.getRuntime().availableProcessors();
                final int threshold = parallel ? Math.max(length() / processors, 10000) : Integer.MAX_VALUE;
                if (count > threshold) {
                    final int splitCount = (to - from) / 2;
                    final int midPoint = from + splitCount;
                    invokeAll(
                        new ApplyValues(from, midPoint, function),
                        new ApplyValues(midPoint+1, to, function)
                    );
                } else if (function instanceof ToBooleanFunction) {
                    final ToBooleanFunction<ArrayValue<T>> typedFunction = (ToBooleanFunction<ArrayValue<T>>)function;
                    final ArrayValueCursor cursor = new ArrayValueCursor();
                    for (int i=from; i<=to; ++i) {
                        cursor.moveTo(i);
                        final boolean result = typedFunction.applyAsBoolean(cursor);
                        ArrayBase.this.setBoolean(i, result);
                    }
                } else if (function instanceof ToIntFunction) {
                    final ToIntFunction<ArrayValue<T>> typedFunction = (ToIntFunction<ArrayValue<T>>)function;
                    final ArrayValueCursor cursor = new ArrayValueCursor();
                    for (int i=from; i<=to; ++i) {
                        cursor.moveTo(i);
                        final int result = typedFunction.applyAsInt(cursor);
                        ArrayBase.this.setInt(i, result);
                    }
                } else if (function instanceof ToLongFunction) {
                    final ToLongFunction<ArrayValue<T>> typedFunction = (ToLongFunction<ArrayValue<T>>)function;
                    final ArrayValueCursor cursor = new ArrayValueCursor();
                    for (int i=from; i<=to; ++i) {
                        cursor.moveTo(i);
                        final long result = typedFunction.applyAsLong(cursor);
                        ArrayBase.this.setLong(i, result);
                    }
                } else if (function instanceof ToDoubleFunction) {
                    final ToDoubleFunction<ArrayValue<T>> typedFunction = (ToDoubleFunction<ArrayValue<T>>)function;
                    final ArrayValueCursor cursor = new ArrayValueCursor();
                    for (int i=from; i<=to; ++i) {
                        cursor.moveTo(i);
                        final double result = typedFunction.applyAsDouble(cursor);
                        ArrayBase.this.setDouble(i, result);
                    }
                } else {
                    final Function<ArrayValue<T>,T> typedFunction = (Function<ArrayValue<T>,T>)function;
                    final ArrayValueCursor cursor = new ArrayValueCursor();
                    for (int i=from; i<=to; ++i) {
                        cursor.moveTo(i);
                        final T result = typedFunction.apply(cursor);
                        ArrayBase.this.setValue(i, result);
                    }
                }
            } catch (Exception ex) {
                throw new DataFrameException("Failed to iterate over Array entries", ex);
            }
        }
    }



    /**
     * A RecursiveAction to apply values in this array to another array
     */
    private class MapValues<X> extends RecursiveAction {

        private int from, to;
        private Object function;
        private Array<X> target;

        /**
         * Constructor
         * @param from      from ordinal
         * @param to        to ordinal
         * @param function  the consumer
         * @param target    the target array
         */
        @SuppressWarnings("unchecked")
        MapValues(int from, int to, Object function, Array<X> target) {
            this.from = from;
            this.to = to;
            this.function = function;
            this.target = target;
            if (from > to) {
                throw new DataFrameException("The to index must be > from index");
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        protected void compute() {
            try {
                final int count = to - from + 1;
                final int processors = Runtime.getRuntime().availableProcessors();
                final int threshold = parallel ? Math.max(length() / processors, 10000) : Integer.MAX_VALUE;
                if (count > threshold) {
                    final int splitCount = (to - from) / 2;
                    final int midPoint = from + splitCount;
                    invokeAll(
                        new MapValues<>(from, midPoint, function, target),
                        new MapValues<>(midPoint+1, to, function, target)
                    );
                } else if (function instanceof ToBooleanFunction) {
                    final ToBooleanFunction<ArrayValue<T>> typedFunction = (ToBooleanFunction<ArrayValue<T>>)function;
                    final ArrayValueCursor cursor = new ArrayValueCursor();
                    for (int i=from; i<=to; ++i) {
                        cursor.moveTo(i);
                        final boolean result = typedFunction.applyAsBoolean(cursor);
                        target.setBoolean(i, result);
                    }
                } else if (function instanceof ToIntFunction) {
                    final ToIntFunction<ArrayValue<T>> typedFunction = (ToIntFunction<ArrayValue<T>>)function;
                    final ArrayValueCursor cursor = new ArrayValueCursor();
                    for (int i=from; i<=to; ++i) {
                        cursor.moveTo(i);
                        final int result = typedFunction.applyAsInt(cursor);
                        target.setInt(i, result);
                    }
                } else if (function instanceof ToLongFunction) {
                    final ToLongFunction<ArrayValue<T>> typedFunction = (ToLongFunction<ArrayValue<T>>)function;
                    final ArrayValueCursor cursor = new ArrayValueCursor();
                    for (int i=from; i<=to; ++i) {
                        cursor.moveTo(i);
                        final long result = typedFunction.applyAsLong(cursor);
                        target.setLong(i, result);
                    }
                } else if (function instanceof ToDoubleFunction) {
                    final ToDoubleFunction<ArrayValue<T>> typedFunction = (ToDoubleFunction<ArrayValue<T>>)function;
                    final ArrayValueCursor cursor = new ArrayValueCursor();
                    for (int i=from; i<=to; ++i) {
                        cursor.moveTo(i);
                        final double result = typedFunction.applyAsDouble(cursor);
                        target.setDouble(i, result);
                    }
                } else {
                    final Function<ArrayValue<T>,X> typedFunction = (Function<ArrayValue<T>,X>)function;
                    final ArrayValueCursor cursor = new ArrayValueCursor();
                    for (int i=from; i<=to; ++i) {
                        cursor.moveTo(i);
                        final X result = typedFunction.apply(cursor);
                        target.setValue(i, result);
                    }
                }
            } catch (Exception ex) {
                throw new DataFrameException("Failed to iterate over Array entries", ex);
            }
        }
    }




    /**
     * A RecursiveAction to iterate over each element in this Array
     */
    private class ForEach extends RecursiveAction {

        private Object consumer;
        private int from, to, splitThreshold;

        /**
         * Constructor
         * @param from              from ordinal
         * @param to                to ordinal
         * @param splitThreshold    the split threshold for this task
         * @param consumer          the consumer for each value
         */
        @SuppressWarnings("unchecked")
        ForEach(int from, int to, int splitThreshold, Object consumer) {
            this.from = from;
            this.to = to;
            this.consumer = consumer;
            this.splitThreshold = splitThreshold;
            if (from > to) {
                throw new DataFrameException("The to index must be > from index");
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        protected void compute() {
            try {
                final int count = to - from + 1;
                if (count > splitThreshold) {
                    final int splitCount = (to - from) / 2;
                    final int midPoint = from + splitCount;
                    invokeAll(
                        new ForEach(from, midPoint, splitThreshold, consumer),
                        new ForEach(midPoint+1, to, splitThreshold, consumer)
                    );
                } else if (consumer instanceof BooleanConsumer) {
                    final BooleanConsumer typedConsumer = (BooleanConsumer)consumer;
                    for (int i=from; i<=to; ++i) {
                        final boolean value = getBoolean(i);
                        typedConsumer.accept(value);
                    }
                } else if (consumer instanceof IntConsumer) {
                    final IntConsumer typedConsumer = (IntConsumer)consumer;
                    for (int i=from; i<=to; ++i) {
                        final int value = getInt(i);
                        typedConsumer.accept(value);
                    }
                } else if (consumer instanceof LongConsumer) {
                    final LongConsumer typedConsumer = (LongConsumer)consumer;
                    for (int i=from; i<=to; ++i) {
                        final long value = getLong(i);
                        typedConsumer.accept(value);
                    }
                } else if (consumer instanceof DoubleConsumer) {
                    final DoubleConsumer typedConsumer = (DoubleConsumer)consumer;
                    for (int i=from; i<=to; ++i) {
                        final double value = getDouble(i);
                        typedConsumer.accept(value);
                    }
                } else {
                    final Consumer<T> typedConsumer = (Consumer<T>)consumer;
                    for (int i=from; i<=to; ++i) {
                        final T value = getValue(i);
                        typedConsumer.accept(value);
                    }
                }
            } catch (Exception ex) {
                throw new DataFrameException("Failed to iterate over Array entries", ex);
            }
        }
    }




    /**
     * A RecursiveAction to iterate over each element in this Array
     */
    private class ForEachArrayValue extends RecursiveAction {

        private int from, to;
        private int splitThreshold;
        private ArrayValueCursor value;
        private Consumer<ArrayValue<T>> consumer;

        /**
         * Constructor
         * @param from      from ordinal
         * @param to        to ordinal
         * @param splitThreshold    the split threshold for this task
         * @param consumer  the value consumer
         */
        @SuppressWarnings("unchecked")
        ForEachArrayValue(int from, int to, int splitThreshold, Consumer<ArrayValue<T>> consumer) {
            this.from = from;
            this.to = to;
            this.splitThreshold = splitThreshold;
            this.consumer = consumer;
            this.value = new ArrayValueCursor();
        }

        @Override
        @SuppressWarnings("unchecked")
        protected void compute() {
            try {
                final int count = to - from + 1;
                if (count > splitThreshold) {
                    final int splitCount = (to - from) / 2;
                    final int midPoint = from + splitCount;
                    invokeAll(
                        new ForEachArrayValue(from, midPoint, splitThreshold, consumer),
                        new ForEachArrayValue(midPoint + 1, to, splitThreshold, consumer)
                    );
                } else {
                    for (int i = from; i <= to; ++i) {
                        value.moveTo(i);
                        consumer.accept(value);
                    }
                }
            } catch (ArrayException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new ArrayException("Failed to iterate over Array entries", ex);
            }
        }
    }




    /**
     * An ArrayValue implementation that can be moved to different locations on the underlying array
     */
    private class ArrayValueCursor implements ArrayCursor<T> {

        private int index;

        /**
         * Constructor
         */
        private ArrayValueCursor() {
            super();
        }

        @Override
        @SuppressWarnings("unchecked")
        public final ArrayCursor<T> copy() {
            try {
                return (ArrayCursor<T>)super.clone();
            } catch (CloneNotSupportedException ex) {
                throw new ArrayException("Failed to clone ArrayValue", ex);
            }
        }

        @Override
        public final ArrayCursor<T> moveTo(int index) {
            this.index = index;
            return this;
        }

        @Override
        public final int index() {
            return index;
        }

        @Override()
        public final Array<T> array() {
            return ArrayBase.this;
        }

        @Override
        public final boolean getBoolean() {
            return ArrayBase.this.getBoolean(index);
        }

        @Override
        public final int getInt() {
            return ArrayBase.this.getInt(index);
        }

        @Override
        public final long getLong() {
            return ArrayBase.this.getLong(index);
        }

        @Override
        public final double getDouble() {
            return ArrayBase.this.getDouble(index);
        }

        @Override
        public final T getValue() {
            return ArrayBase.this.getValue(index);
        }

        @Override
        public final void setBoolean(boolean value) {
            ArrayBase.this.setBoolean(index, value);
        }

        @Override
        public final void setInt(int value) {
            ArrayBase.this.setInt(index, value);
        }

        @Override
        public final void setLong(long value) {
            ArrayBase.this.setLong(index, value);
        }

        @Override
        public final void setDouble(double value) {
            ArrayBase.this.setDouble(index, value);
        }

        @Override
        public final void setValue(T value) {
            ArrayBase.this.setValue(index, value);
        }

        @Override
        public boolean isNull() {
            return ArrayBase.this.isNull(index);
        }

        @Override
        public boolean isEqualTo(T value) {
            return ArrayBase.this.isEqualTo(index, value);
        }
    }


    /**
     * Implementation of the ArrayStreams interface
     * @param <X>   the array element type
     */
    private class Streams<X> implements ArrayStreams<X> {

        private final int start;
        private final int end;
        private final Array<X> array;

        /**
         * Constructor
         * @param array the array to operate on
         * @param start the start index for stream, inclusive
         * @param end   the end index for stream, exclusive
         */
        Streams(Array<X> array, int start, int end) {
            this.array = array;
            this.start = start;
            this.end = end;
        }

        @Override
        public IntStream ints() {
            return IntStream.range(start, end).map(array::getInt);
        }

        @Override
        public LongStream longs() {
            return IntStream.range(start, end).mapToLong(array::getLong);
        }

        @Override
        public DoubleStream doubles() {
            return IntStream.range(start, end).mapToDouble(array::getDouble);
        }

        @Override
        public final Stream<X> values() {
            return IntStream.range(start, end).mapToObj(array::getValue);
        }
    }

    /**
     * A java.util.List wrapper of a Morpheus Array supporting a subset of the List operations.
     * @param <X>   the element type of array
     */
    private class ListWrapper<X> extends AbstractList<X> implements List<X> , java.io.Serializable{

        private static final long serialVersionUID = 1L;

        private Array<X> array;

        /**
         * Constructor
         * @param array the array to wrap
         */
        private ListWrapper(Array<X> array) {
            this.array = array;
        }

        @Override
        public int size() {
            return array.length();
        }

        @Override
        public X get(int index) {
            return array.getValue(index);
        }

        @Override
        public boolean add(X t) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public void add(int index, X element) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public X remove(int index) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        public boolean addAll(int index, Collection<? extends X> c) {
            throw new UnsupportedOperationException("This list is immutable");
        }

        @Override
        protected void removeRange(int fromIndex, int toIndex) {
            throw new UnsupportedOperationException("This list is immutable");
        }
    }


    /**
     * An IntComparator implementation that calls a user provided Comparator with ArrayValue objects
     */
    private class ArrayIntComparator implements IntComparator {

        private ArrayValueCursor v1;
        private ArrayValueCursor v2;
        private Comparator<ArrayValue<T>> comp;

        /**
         * Constructor
         * @param comp  the user provided comparator
         */
        ArrayIntComparator(Comparator<ArrayValue<T>> comp) {
            this.v1 = (ArrayValueCursor)cursor();
            this.v2 = (ArrayValueCursor)cursor();
            this.comp = comp;
        }

        @Override
        public int compare(int index1, int index2) {
            this.v1.moveTo(index1);
            this.v2.moveTo(index2);
            return comp.compare(v1, v2);
        }

        @Override
        public IntComparator copy() {
            return new ArrayIntComparator(comp);
        }
    }

}
