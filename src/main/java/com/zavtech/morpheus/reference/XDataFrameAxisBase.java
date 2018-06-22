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
package com.zavtech.morpheus.reference;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBuilder;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameAxis;
import com.zavtech.morpheus.frame.DataFrameColumn;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameOptions;
import com.zavtech.morpheus.frame.DataFrameRow;
import com.zavtech.morpheus.frame.DataFrameVector;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.util.Asserts;
import com.zavtech.morpheus.util.Parallel;
import com.zavtech.morpheus.util.Tuple;

/**
 * A convenience base class for building DataFrameAxis implementations to expose bulk operations on the row and column dimension
 *
 * @param <X>   the key type for this axis
 * @param <Y>   the opposing dimension key type
 * @param <R>   the key type of the row dimension
 * @param <C>   the key type of the column dimension
 * @param <V>   the vector type for this dimension
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
abstract class XDataFrameAxisBase<X,Y,R,C,V extends DataFrameVector<?,?,R,C,?>,T extends DataFrameAxis<X,Y,R,C,V,T,G>,G> implements DataFrameAxis<X,Y,R,C,V,T,G> {

    private Type axisType;
    private boolean parallel;
    private Index<X> axis;
    private XDataFrame<R,C> frame;

    /**
     * Constructor
     * @param frame     the frame to operate on
     * @param parallel  true for parallel implementation
     * @param row       true if row dimension, false for column dimension
     */
    @SuppressWarnings("unchecked")
    XDataFrameAxisBase(XDataFrame<R,C> frame, boolean parallel, boolean row) {
        this.frame = frame;
        this.parallel = parallel;
        this.axisType = row ? Type.ROWS : Type.COLS;
        this.axis = row ? (Index<X>)frame.rowKeys() : (Index<X>)frame.colKeys();
    }


    /**
     * Returns a newly created vector for this axis
     * @param frame     the frame reference to pass to vector
     * @param ordinal   the ordinal for the vector
     * @return          the newly created vector
     */
    @SuppressWarnings("unchecked")
    private V createVector(XDataFrame<R,C> frame, int ordinal) {
        switch(axisType) {
            case ROWS:  return (V)new XDataFrameRow<>(frame, isParallel(), ordinal);
            case COLS:  return (V)new XDataFrameColumn<>(frame, isParallel(), ordinal);
            default:    throw new DataFrameException("Unsupported axis type: " + axisType);
        }
    }

    /**
     * Returns a newly created filter over the frame based on the keys specified
     * @param frame the source frame to filter
     * @param keys  the keys for the new filter axis
     * @return      the newly created frame filter
     */
    @SuppressWarnings("unchecked")
    private DataFrame<R,C> createFilter(XDataFrame<R,C> frame, Iterable<X> keys) {
        if (axisType.isRow()) {
            final Index<R> newRowKeys = frame.rowKeys().filter((Iterable<R>)keys);
            final Index<C> newColKeys = frame.colKeys().copy();
            return frame.filter(newRowKeys, newColKeys);
        } else {
            final Index<R> newRowKeys = frame.rowKeys().copy();
            final Index<C> newColKeys = frame.colKeys().filter((Iterable<C>)keys);
            return frame.filter(newRowKeys, newColKeys);
        }
    }

    /**
     * Returns a reference to the frame to which this axis belongs
     * @return  the frame to which this axis belongs
     */
    protected XDataFrame<R,C> frame() {
        return frame;
    }

    @Override
    public final int count() {
        return axis.size();
    }

    @Override
    public final X key(int ordinal) {
        return axis.getKey(ordinal);
    }

    @Override
    public final Stream<X> keys() {
        return axis.keys();
    }

    @Override
    public final Class<X> keyType() {
        return axis.type();
    }

    @Override
    public final Array<X> keyArray() {
        return axis.toArray();
    }

    @Override
    public final IntStream ordinals() {
        return IntStream.range(0, axis.size());
    }

    @Override
    public boolean isEmpty() {
        return count() == 0;
    }

    @Override
    public boolean isParallel() {
        return parallel;
    }

    @Override
    public final Optional<X> firstKey() {
        return axis.first();
    }

    @Override
    public final Optional<X> lastKey() {
        return axis.last();
    }

    @Override
    public final Optional<X> lowerKey(X key) {
        return axis.previousKey(key);
    }

    @Override
    public final Optional<X> higherKey(X key) {
        return axis.nextKey(key);
    }

    @Override
    public final int ordinalOf(X key) {
        return axis.getOrdinalForKey(key);
    }

    @Override
    public final int ordinalOf(X key, boolean strict) {
        return axis.getOrdinalForKey(key);
    }

    @Override
    public final boolean contains(X key) {
        return axis.contains(key);
    }

    @Override
    public final boolean containsAll(Iterable<X> keys) {
        return axis.containsAll(keys);
    }

    @Override
    public final Optional<V> first() {
        return count() > 0 ? Optional.of(createVector(frame, 0)) : Optional.empty();
    }

    @Override
    public final Optional<V> last() {
        return count() > 0 ? Optional.of(createVector(frame, count()-1)) : Optional.empty();
    }

    @Override
    public final Stream<V> stream() {
        if (count() == 0) {
            return Stream.empty();
        } else if (axisType == Type.ROWS) {
            final int rowCount = frame.rowCount();
            final int partitionSize = rowCount / Runtime.getRuntime().availableProcessors();
            final int splitThreshold = Math.max(partitionSize, 10000);
            return StreamSupport.stream(new DataFrameVectorSpliterator<>(0, rowCount-1, rowCount, splitThreshold), frame.isParallel());
        } else if (axisType == Type.COLS) {
            final int colCount = frame.colCount();
            final int partitionSize = colCount / Runtime.getRuntime().availableProcessors();
            final int splitThreshold = Math.max(partitionSize, 10000);
            return StreamSupport.stream(new DataFrameVectorSpliterator<>(0, colCount-1, colCount, splitThreshold), frame.isParallel());
        } else {
            throw new DataFrameException("Unsupported axis type: " + axisType);
        }
    }

    @Override
    public Stream<Class<?>> types() {
        switch (axisType) {
            case ROWS:  return frame.content().rowTypes();
            case COLS:  return frame.content().colTypes();
            default:    throw new DataFrameException("Unsupported axis type: " + axisType);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public final Class<?> type(X key) {
        switch (axisType) {
            case ROWS:  return frame.content().rowType((R)key);
            case COLS:  return frame.content().colType((C)key);
            default:    throw new DataFrameException("Unsupported axis type: " + axisType);
        }
    }


    @Override
    public final Iterator<V> iterator() {
        final V vector = createVector(frame, 0);
        return new Iterator<V>() {
            private int ordinal;
            @Override
            public final boolean hasNext() {
                return ordinal < count();
            }
            @Override
            @SuppressWarnings("unchecked")
            public final V next() {
                ((XDataFrameVector)vector).moveTo(ordinal++);
                return vector;
            }
        };
    }


    @Override
    @Parallel
    public final void forEach(Consumer<? super V> consumer) {
        if (parallel) {
            final int count = count();
            final ForEachVector action = new ForEachVector(0, count - 1, consumer);
            ForkJoinPool.commonPool().invoke(action);
        } else if (count() > 0) {
            final int count = count();
            final V vector = createVector(frame, 0);
            for (int ordinal=0; ordinal < count; ++ordinal) {
                ((XDataFrameVector)vector).moveTo(ordinal);
                consumer.accept(vector);
            }
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public final G groupBy(Y... keys) {
        switch (axisType) {
            case ROWS:  return (G)XDataFrameGroupingRows.of(frame, isParallel(), (Array<C>)Array.of(keys));
            case COLS:  return (G)XDataFrameGroupingCols.of(frame, isParallel(), (Array<R>)Array.of(keys));
            default:    throw new DataFrameException("Unsupported axis type: " + axisType);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public final G groupBy(Function<V,Tuple> function) {
        switch (axisType) {
            case ROWS:  return (G)XDataFrameGroupingRows.of(frame, isParallel(), (Function<DataFrameRow<R,C>,Tuple>)function);
            case COLS:  return (G)XDataFrameGroupingCols.of(frame, isParallel(), (Function<DataFrameColumn<R,C>,Tuple>)function);
            default:    throw new DataFrameException("Unsupported axis type: " + axisType);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public T filter(X... keys) {
        switch (axisType) {
            case ROWS:  return (T)select(keys).rows();
            case COLS:  return (T)select(keys).cols();
            default:    throw new DataFrameException("Unsupported axis type: " + axisType);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public final T filter(Iterable<X> keys) {
        switch (axisType) {
            case ROWS:  return (T)select(keys).rows();
            case COLS:  return (T)select(keys).cols();
            default:    throw new DataFrameException("Unsupported axis type: " + axisType);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public final T filter(Predicate<V> predicate) {
        switch (axisType) {
            case ROWS:  return (T)select(predicate).rows();
            case COLS:  return (T)select(predicate).cols();
            default:    throw new DataFrameException("Unsupported axis type: " + axisType);
        }
    }


    @Override
    public final DataFrame<R,C> replaceKey(X key, X newKey) {
        if (axis.isFilter() && axisType == Type.ROWS) {
            throw new DataFrameException("Row axis is immutable for this frame, call copy() first");
        } else if (axis.isFilter() && axisType == Type.COLS) {
            throw new DataFrameException("Column axis is immutable for this frame, call copy() first");
        } else {
            this.axis.replace(key, newKey);
            return frame;
        }
    }


    @Override
    @SafeVarargs
    public final DataFrame<R,C> select(X... keys) {
        return createFilter(frame, Array.of(keys));
    }


    @Override
    public final DataFrame<R,C> select(Iterable<X> keys) {
        return createFilter(frame, keys);
    }


    @Override
    @Parallel
    public final DataFrame<R,C> select(Predicate<V> predicate) {
        if (parallel) {
            final int count = count();
            final Select select = new Select(0, count-1, predicate);
            final Array<X> keys = ForkJoinPool.commonPool().invoke(select);
            return createFilter(frame, keys);
        } else {
            final int count = count();
            if (count == 0) {
                return createFilter(frame, Collections.emptyList());
            } else {
                final Select select = new Select(0, count-1, predicate);
                final Array<X> keys = select.compute();
                return createFilter(frame, keys);
            }
        }
    }


    @Override
    public final Optional<V> first(Predicate<V> predicate) {
        final int count = count();
        final V vector = createVector(frame, 0);
        for (int ordinal=0; ordinal < count; ++ordinal) {
            ((XDataFrameVector)vector).moveTo(ordinal);
            if (predicate.test(vector)) {
                return Optional.of(vector);
            }
        }
        return Optional.empty();
    }


    @Override
    public final Optional<V> last(Predicate<V> predicate) {
        final int count = count();
        final V vector = createVector(frame, 0);
        for (int ordinal=count-1; ordinal >= 0; --ordinal) {
            ((XDataFrameVector)vector).moveTo(ordinal);
            if (predicate.test(vector)) {
                return Optional.of(vector);
            }
        }
        return Optional.empty();
    }


    @Override
    public final Optional<V> min(Comparator<V> comparator) {
        final MinVector task = new MinVector(0, count()-1, comparator);
        final V result = parallel ? ForkJoinPool.commonPool().invoke(task) : task.compute();
        return Optional.ofNullable(result);
    }


    @Override
    public final Optional<V> max(Comparator<V> comparator) {
        final maxVector task = new maxVector(0, count()-1, comparator);
        final V result = parallel ? ForkJoinPool.commonPool().invoke(task) : task.compute();
        return Optional.ofNullable(result);
    }



    /**
     * A RecursiveAction to iterate over vectors in a DataFrame, which could either be row or column vectors
     */
    private class ForEachVector extends RecursiveAction {

        private int from;
        private int to;
        private V vector;
        private Consumer<? super V> consumer;

        /**
         * Constructor
         * @param from      from ordinal
         * @param to        to ordinal
         * @param consumer  the vector consumer
         */
        @SuppressWarnings("unchecked")
        ForEachVector(int from, int to, Consumer<? super V> consumer) {
            this.from = from;
            this.to = to;
            this.consumer = consumer;
            this.vector = createVector(frame, 0);
            if (from > to) {
                throw new DataFrameException("The to index must be > from index");
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        protected void compute() {
            try {
                final int count = to - from + 1;
                final int threshold = parallel ? DataFrameOptions.getRowSplitThreshold(frame) : Integer.MAX_VALUE;
                if (count <= threshold) {
                    for (int i=from; i<=to; ++i) {
                        ((XDataFrameVector)vector).moveTo(i);
                        this.consumer.accept(vector);
                    }
                } else {
                    final int splitCount = (to - from) / 2;
                    final int midPoint = from + splitCount;
                    invokeAll(
                        new ForEachVector(from, midPoint, consumer),
                        new ForEachVector(midPoint+1, to, consumer)
                    );
                }
            } catch (Exception ex) {
                throw new DataFrameException("Failed to iterate over DataFrame axis vectors", ex);
            }
        }
    }


    /**
     * A RecursiveTask that implements a parallel select of keys that match a predicate while preserving order
     */
    private class Select extends RecursiveTask<Array<X>> {

        private int from;
        private int to;
        private int threshold;
        private Predicate<V> predicate;

        /**
         * Constructor
         * @param from      the from ordinal
         * @param to        the to row index in view space
         * @param predicate the predicate to match rows
         */
        Select(int from, int to, Predicate<V> predicate) {
            this.from = from;
            this.to = to;
            this.predicate = predicate;
            this.threshold = Integer.MAX_VALUE;
            if (isParallel()) {
                switch (axisType) {
                    case ROWS:  this.threshold = DataFrameOptions.getRowSplitThreshold(frame);      break;
                    case COLS:  this.threshold = DataFrameOptions.getColumnSplitThreshold(frame);   break;
                }
            }
        }

        @Override
        protected Array<X> compute() {
            final int count = to - from + 1;
            final Class<X> keyType = keyType();
            if (count > threshold) {
                return split();
            } else {
                final int rowCount = count();
                final V vector = createVector(frame, 0);
                final ArrayBuilder<X> builder = ArrayBuilder.of(rowCount > 0 ? rowCount : 10, keyType);
                for (int ordinal=from; ordinal<=to; ++ordinal) {
                    ((XDataFrameVector)vector).moveTo(ordinal);
                    if (predicate.test(vector)) {
                        builder.add((X)vector.key());
                    }
                }
                return builder.toArray();
            }
        }

        /**
         * Splits this task into two sub-tasks and executes them in parallel
         * @return  the join results from the two sub-tasks
         */
        private Array<X> split() {
            final int splitCount = (to - from) / 2;
            final int midPoint = from + splitCount;
            final Select left  = new Select(from, midPoint, predicate);
            final Select right = new Select(midPoint + 1, to, predicate);
            left.fork();
            final Array<X> rightAns = right.compute();
            final Array<X> leftAns  = left.join();
            final int size = Math.max(rightAns.length() + leftAns.length(), 10);
            final ArrayBuilder<X> builder = ArrayBuilder.of(size, keyType());
            builder.addAll(leftAns);
            builder.addAll(rightAns);
            return builder.toArray();
        }
    }


    /**
     * A Spliterator implementation to iterate over all vectors in this axis
     */
    private class DataFrameVectorSpliterator<A> implements Spliterator<A> {

        private A vector;
        private int position;
        private int start;
        private int end;
        private int count;
        private int splitThreshold;

        /**
         * Constructor
         * @param start             the start ordinal
         * @param end               the end ordinal
         * @param count             the row or column count if this represents a row or column axis
         * @param splitThreshold    the split threshold
         */
        @SuppressWarnings("unchecked")
        private DataFrameVectorSpliterator(int start, int end, int count, int splitThreshold) {
            Asserts.check(start <= end, "The from ordinal must be <= the to oridinal");
            Asserts.check(splitThreshold > 0, "The split threshold must be > 0");
            this.position = start;
            this.start = start;
            this.end = end;
            this.count = count;
            this.splitThreshold = splitThreshold;
            this.vector = (A)createVector(frame, start);
        }

        @Override
        public boolean tryAdvance(Consumer<? super A> action) {
            Asserts.check(action != null, "The consumer action cannot be null");
            if (position <= end) {
                if (vector instanceof XDataFrameRow) {
                    ((XDataFrameRow)vector).moveTo(position);
                    ++position;
                    action.accept(vector);
                    return true;
                } else if (vector instanceof XDataFrameColumn) {
                    ((XDataFrameColumn)vector).moveTo(position);
                    ++position;
                    action.accept(vector);
                    return true;
                } else {
                    throw new DataFrameException("Unsupported vector type: " + vector.getClass());
                }
            } else {
                return false;
            }
        }

        @Override
        public Spliterator<A> trySplit() {
            if (estimateSize() < splitThreshold) {
                return null;
            } else {
                final int newStart = start;
                final int halfSize = (end - start) / 2;
                final int newEnd = newStart + halfSize;
                this.start = newEnd + 1;
                this.position = start;
                return new DataFrameVectorSpliterator<>(newStart, newEnd, count, splitThreshold);
            }
        }

        @Override
        public long estimateSize() {
            return getExactSizeIfKnown();
        }

        @Override
        public int characteristics() {
            return SIZED | IMMUTABLE | SUBSIZED | CONCURRENT;
        }

        @Override
        public long getExactSizeIfKnown() {
            return (end - start) + 1;
        }
    }


    /**
     * A RecursiveTask implementation that determines the min() of rows or columns
     */
    private class MinVector extends RecursiveTask<V> {

        private int fromOrdinal;
        private int toOrdinal;
        private int threshold;
        private Comparator<V> comparator;

        /**
         * Constructor
         * @param fromOrdinal       the from ordinal
         * @param toOrdinal         the to ordinal
         * @param comparator        the comparator that defines order
         */
        MinVector(int fromOrdinal, int toOrdinal, Comparator<V> comparator) {
            Asserts.check(fromOrdinal >= 0, "from ordinal must be > 0");
            Asserts.check(toOrdinal >= 0, "to ordinal must be > 0");
            Asserts.check(toOrdinal > fromOrdinal, "The toOrdinal must be > fromOrdinal");
            this.fromOrdinal = fromOrdinal;
            this.toOrdinal = toOrdinal;
            this.comparator = comparator;
            this.threshold = Integer.MAX_VALUE;
            if (parallel) {
                this.threshold = Math.max(1000, count() / Runtime.getRuntime().availableProcessors());
            }
        }

        @Override
        protected V compute() {
            if (count() == 0) return null;
            final int count = toOrdinal - fromOrdinal + 1;
            if (count > threshold) {
                final int partitionSize = (toOrdinal - fromOrdinal) / 2;
                final int midPoint = fromOrdinal + partitionSize;
                final MinVector left  = new MinVector(fromOrdinal, midPoint, comparator);
                final MinVector right = new MinVector(midPoint + 1, toOrdinal, comparator);
                left.fork();
                final V rightAns = right.compute();
                final V leftAns  = left.join();
                final int compare = comparator.compare(leftAns, rightAns);
                return compare > 0 ? rightAns : leftAns;
            } else {
                return argMin();
            }
        }

        /**
         * Returns the min row or column vector
         * @return  the min row or column vector
         */
        private V argMin() {
            V minVector = createVector(frame, fromOrdinal);
            final V otherVector = createVector(frame, fromOrdinal);
            for (int i=fromOrdinal+1; i<=toOrdinal; ++i) {
                ((XDataFrameVector)otherVector).moveTo(i);
                final int compare = comparator.compare(minVector, otherVector);
                if (compare > 0) ((XDataFrameVector)minVector).moveTo(i);
            }
            return minVector;
        }
    }



    /**
     * A RecursiveTask implementation that determines the max() of rows or columns
     */
    private class maxVector extends RecursiveTask<V> {

        private int fromOrdinal;
        private int toOrdinal;
        private int threshold;
        private Comparator<V> comparator;

        /**
         * Constructor
         * @param fromOrdinal       the from ordinal
         * @param toOrdinal         the to ordinal
         * @param comparator        the comparator that defines order
         */
        maxVector(int fromOrdinal, int toOrdinal, Comparator<V> comparator) {
            this.fromOrdinal = fromOrdinal;
            this.toOrdinal = toOrdinal;
            this.comparator = comparator;
            this.threshold = Integer.MAX_VALUE;
            if (parallel) {
                this.threshold = Math.max(1000, count() / Runtime.getRuntime().availableProcessors());
            }
        }

        @Override
        protected V compute() {
            if (count() == 0) return null;
            final int count = toOrdinal - fromOrdinal + 1;
            if (count > threshold) {
                final int partitionSize = (toOrdinal - fromOrdinal) / 2;
                final int midPoint = fromOrdinal + partitionSize;
                final maxVector left  = new maxVector(fromOrdinal, midPoint, comparator);
                final maxVector right = new maxVector(midPoint + 1, toOrdinal, comparator);
                left.fork();
                final V rightAns = right.compute();
                final V leftAns  = left.join();
                final int compare = comparator.compare(leftAns, rightAns);
                return compare < 0 ? rightAns : leftAns;
            } else {
                return argMax();
            }
        }

        /**
         * Returns the min row or column vector
         * @return  the min row or column vector
         */
        private V argMax() {
            V maxVector = createVector(frame, fromOrdinal);
            final V otherVector = createVector(frame, fromOrdinal);
            for (int i=fromOrdinal+1; i<=toOrdinal; ++i) {
                ((XDataFrameVector)otherVector).moveTo(i);
                final int compare = comparator.compare(otherVector, maxVector);
                if (compare > 0) ((XDataFrameVector)maxVector).moveTo(i);
            }
            return maxVector;
        }
    }


}
