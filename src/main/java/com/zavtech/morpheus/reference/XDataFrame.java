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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBuilder;
import com.zavtech.morpheus.array.ArrayUtils;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameAlgebra;
import com.zavtech.morpheus.frame.DataFrameCalculate;
import com.zavtech.morpheus.frame.DataFrameCap;
import com.zavtech.morpheus.frame.DataFrameColumn;
import com.zavtech.morpheus.frame.DataFrameColumns;
import com.zavtech.morpheus.frame.DataFrameContent;
import com.zavtech.morpheus.frame.DataFrameCursor;
import com.zavtech.morpheus.frame.DataFrameEvents;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameExport;
import com.zavtech.morpheus.frame.DataFrameFill;
import com.zavtech.morpheus.frame.DataFrameOptions;
import com.zavtech.morpheus.frame.DataFrameOutput;
import com.zavtech.morpheus.frame.DataFramePCA;
import com.zavtech.morpheus.frame.DataFrameRank;
import com.zavtech.morpheus.frame.DataFrameRegression;
import com.zavtech.morpheus.frame.DataFrameRow;
import com.zavtech.morpheus.frame.DataFrameRows;
import com.zavtech.morpheus.frame.DataFrameSmooth;
import com.zavtech.morpheus.frame.DataFrameValue;
import com.zavtech.morpheus.frame.DataFrameWrite;
import com.zavtech.morpheus.index.IndexMapper;
import com.zavtech.morpheus.reference.algebra.XDataFrameAlgebra;
import com.zavtech.morpheus.reference.regress.XDataFrameRegression;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.stats.Sample;
import com.zavtech.morpheus.stats.Stats;
import com.zavtech.morpheus.util.Asserts;
import com.zavtech.morpheus.util.Bounds;
import com.zavtech.morpheus.util.functions.ToBooleanFunction;
import com.zavtech.morpheus.util.text.Formats;

/**
 * The reference implementation of the DataFrame interface.
 *
 * @param <C>   the column key type
 * @param <R>   the row key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrame<R,C> implements DataFrame<R,C>, Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    private boolean parallel;
    private XDataFrameEvents events;
    private XDataFrameRows<R,C> rows;
    private XDataFrameColumns<R,C> cols;
    private XDataFrameContent<R,C> data;


    /**
     * Constructor
     * @param rowKeys   the row keys for DataFrame
     * @param colKeys   the column keys for DataFrame
     * @param type      the data type for columns
     * @param parallel  true for parallel implementation
     */
    XDataFrame(Index<R> rowKeys, Index<C> colKeys, Class<?> type, boolean parallel) {
        this(new XDataFrameContent<>(rowKeys, colKeys, type), parallel);
    }

    /**
     * Private constructor used to create DataFrame filtered views
     * @param data      the data content for this DataFrame
     * @param parallel  true for parallel implementation
     */
    XDataFrame(XDataFrameContent<R,C> data, boolean parallel) {
        this.data = data;
        this.parallel = parallel;
        this.events = new XDataFrameEvents();
        this.rows = new XDataFrameRows<>(this, parallel);
        this.cols = new XDataFrameColumns<>(this, parallel);
    }


    /**
     * Configures columns based on the consumer
     * @param consumer  the column consumer
     * @return          this frame
     */
    final XDataFrame<R,C> configure(Consumer<DataFrameColumns<R,C>> consumer) {
        consumer.accept(cols());
        return this;
    }


    /**
     * Returns a shallow copy of the frame replacing the row keys
     * @param mapper    the mapper to map row keys
     * @param <X>       the new row key type
     * @return          the shallow copy of the frame
     */
    final <X> XDataFrame<X,C> mapRowKeys(IndexMapper<R,X> mapper) {
        return new XDataFrame<>(data.mapRowKeys(mapper), isParallel());
    }


    /**
     * Returns a shallow copy of the frame replacing the column keys
     * @param mapper    the mapper to map column keys
     * @param <Y>       the new row key type
     * @return          the shallow copy of the frame
     */
    final <Y> XDataFrame<R,Y> mapColKeys(IndexMapper<C,Y> mapper) {
        return new XDataFrame<>(data.mapColKeys(mapper), isParallel());
    }


    /**
     * Returns a filter of this frame based on the row and column dimensions provided
     * @param rowKeys   the row keys for frame, which could include a subset of row keys
     * @param colKeys   the column keys for frame, which could include a subset of column keys
     */
    final XDataFrame<R,C> filter(Index<R> rowKeys, Index<C> colKeys) {
        return new XDataFrame<>(data.filter(rowKeys, colKeys), parallel);
    }


    /**
     * Returns the algebraic interface for this DataFrame
     * @return      the algebraic interface
     */
    private DataFrameAlgebra<R,C> algebra() {
        return XDataFrameAlgebra.create(this);
    }


    /**
     * Returns a reference to the index of row keys
     * @return  the row keys
     */
    final Index<R> rowKeys() {
        return data.rowKeyIndex();
    }


    /**
     * Returns a reference to the index of column keys
     * @return  the column keys
     */
    final Index<C> colKeys() {
        return data.colKeyIndex();
    }


    /**
     * Returns direct access to the contents for this frame
     * @return  the contents for this frame
     */
    final XDataFrameContent<R,C> content() {
        return data;
    }


    /**
     * Returns a Sample representation of this DataFrame for statistical calculations
     * @return      the sample interface over this DataFrame
     */
    private Sample toSample() {
        return index -> {
            if (index == 0) {
                return data.getDouble(0, 0);
            } else {
                final int rowCount = rows.count();
                final int rowIndex = index % rowCount;
                final int colIndex = index / rowCount;
                return data.getDouble(rowIndex, colIndex);
            }
        };
    }

    @Override()
    public final DataFrame<R,C> parallel() {
        return parallel ? this : new XDataFrame<>(data, true);
    }


    @Override()
    public final DataFrame<R,C> sequential() {
        return parallel ? new XDataFrame<>(data, false) : this;
    }


    @Override()
    @SuppressWarnings("unchecked")
    public final DataFrame<R,C> copy() {
        try {
            final XDataFrame<R,C> clone = (XDataFrame<R,C>)super.clone();
            clone.data = this.data.copy();
            clone.events = new XDataFrameEvents();
            clone.rows = new XDataFrameRows<>(clone, parallel);
            clone.cols = new XDataFrameColumns<>(clone, parallel);
            return clone;
        } catch (CloneNotSupportedException ex) {
            throw new DataFrameException("Failed to create a deep copy of DataFrame", ex);
        }
    }


    @Override()
    public final int rowCount() {
        return rowKeys().size();
    }


    @Override()
    public final int colCount() {
        return colKeys().size();
    }


    @Override()
    public final boolean isParallel() {
        return parallel;
    }


    @Override
    public final DataFrameContent<R,C> data() {
        return data;
    }


    @Override()
    public final DataFrameRows<R,C> rows() {
        return rows;
    }


    @Override()
    public final DataFrameColumns<R,C> cols() {
        return cols;
    }


    @Override()
    public final DataFrameCursor<R,C> cursor() {
        return data.cursor(this);
    }


    @Override
    public final DataFrameRow<R,C> row(R rowKey) {
        return new XDataFrameRow<>(this, parallel, rowKeys().getOrdinalForKey(rowKey));
    }


    @Override
    public final DataFrameRow<R,C> rowAt(int rowOrdinal) {
        return new XDataFrameRow<>(this, parallel, rowOrdinal);
    }


    @Override
    public final DataFrameColumn<R,C> col(C colKey) {
        return new XDataFrameColumn<>(this, parallel, colKeys().getOrdinalForKey(colKey));
    }


    @Override
    public final DataFrameColumn<R,C> colAt(int colIOrdinal) {
        return new XDataFrameColumn<>(this, parallel, colIOrdinal);
    }


    @Override
    public int count(Predicate<DataFrameValue<R,C>> predicate) {
        final AtomicInteger count = new AtomicInteger();
        this.forEachValue(v -> {
            if (predicate.test(v)) {
                count.incrementAndGet();
            }
        });
        return count.get();
    }


    @Override()
    public <V> Optional<V> min(Predicate<DataFrameValue<R,C>> predicate) {
        if (rowCount() == 0 || colCount() == 0) {
            return Optional.empty();
        } else if (rowCount() > colCount()) {
            final MinMaxValueTask task = new MinMaxValueTask(0, rowCount(), true, predicate);
            final Optional<DataFrameValue<R,C>> result = isParallel() ? ForkJoinPool.commonPool().invoke(task) : task.compute();
            return result.map(DataFrameValue::<V>getValue);
        } else {
            final MinMaxValueTask task = new MinMaxValueTask(0, colCount(), true, predicate);
            final Optional<DataFrameValue<R,C>> result = isParallel() ? ForkJoinPool.commonPool().invoke(task) : task.compute();
            return result.map(DataFrameValue::<V>getValue);
        }
    }


    @Override()
    public <V> Optional<V> max(Predicate<DataFrameValue<R,C>> predicate) {
        if (rowCount() == 0 || colCount() == 0) {
            return Optional.empty();
        } else if (rowCount() > colCount()) {
            final MinMaxValueTask task = new MinMaxValueTask(0, rowCount(), false, predicate);
            final Optional<DataFrameValue<R,C>> result = isParallel() ? ForkJoinPool.commonPool().invoke(task) : task.compute();
            return result.map(DataFrameValue::<V>getValue);
        } else {
            final MinMaxValueTask task = new MinMaxValueTask(0, colCount(), false, predicate);
            final Optional<DataFrameValue<R,C>> result = isParallel() ? ForkJoinPool.commonPool().invoke(task) : task.compute();
            return result.map(DataFrameValue::<V>getValue);
        }
    }


    @Override
    public <V> Optional<Bounds<V>> bounds(Predicate<DataFrameValue<R, C>> predicate) {
        if (rowCount() == 0 || colCount() == 0) {
            return Optional.empty();
        } else if (rowCount() > colCount()) {
            final BoundsTask<V> task = new BoundsTask<>(0, rowCount(), predicate);
            return isParallel() ? ForkJoinPool.commonPool().invoke(task) : task.compute();
        } else {
            final BoundsTask<V> task = new BoundsTask<>(0, colCount(), predicate);
            return isParallel() ? ForkJoinPool.commonPool().invoke(task) : task.compute();
        }
    }


    @Override()
    public final DataFrame<R,C> forEachValue(Consumer<DataFrameValue<R,C>> consumer) {
        if (parallel && colCount() > 0) {
            final int toIndex = rowCount() * colCount() - 1;
            final int threshold = (rowCount() * colCount()) / Runtime.getRuntime().availableProcessors();
            final ForEachValue action = new ForEachValue(0, toIndex, threshold, consumer);
            ForkJoinPool.commonPool().invoke(action);
        } else if (colCount() > 0) {
            final int toIndex = rowCount() * colCount() - 1;
            final int threshold = Integer.MAX_VALUE;
            final ForEachValue action = new ForEachValue(0, toIndex, threshold, consumer);
            action.compute();
        }
        return this;
    }


    @Override()
    public final DataFrame<R,C> applyBooleans(ToBooleanFunction<DataFrameValue<R,C>> mapper) {
        if (parallel && colCount() > 0) {
            final int toIndex = rowCount() * colCount() - 1;
            final int threshold = (rowCount() * colCount()) / Runtime.getRuntime().availableProcessors();
            final ApplyBooleans action = new ApplyBooleans(0, toIndex, threshold, mapper);
            ForkJoinPool.commonPool().invoke(action);
        } else if (colCount() > 0) {
            final int toIndex = rowCount() * colCount() - 1;
            final int threshold = Integer.MAX_VALUE;
            final ApplyBooleans action = new ApplyBooleans(0, toIndex, threshold, mapper);
            action.compute();
        }
        return this;
    }


    @Override()
    public final DataFrame<R,C> applyInts(ToIntFunction<DataFrameValue<R,C>> mapper) {
        if (parallel && colCount() > 0) {
            final int toIndex = rowCount() * colCount() - 1;
            final int threshold = (rowCount() * colCount()) / Runtime.getRuntime().availableProcessors();
            final ApplyInts action = new ApplyInts(0, toIndex, threshold, mapper);
            ForkJoinPool.commonPool().invoke(action);
        } else if (colCount() > 0) {
            final int toIndex = rowCount() * colCount() - 1;
            final int threshold = Integer.MAX_VALUE;
            final ApplyInts action = new ApplyInts(0, toIndex, threshold, mapper);
            action.compute();
        }
        return this;
    }


    @Override()
    public final DataFrame<R,C> applyLongs(ToLongFunction<DataFrameValue<R,C>> mapper) {
        if (parallel && colCount() > 0) {
            final int toIndex = rowCount() * colCount() - 1;
            final int threshold = (rowCount() * colCount()) / Runtime.getRuntime().availableProcessors();
            final ApplyLongs action = new ApplyLongs(0, toIndex, threshold, mapper);
            ForkJoinPool.commonPool().invoke(action);
        } else if (colCount() > 0) {
            final int toIndex = rowCount() * colCount() - 1;
            final int threshold = Integer.MAX_VALUE;
            final ApplyLongs action = new ApplyLongs(0, toIndex, threshold, mapper);
            action.compute();
        }
        return this;
    }


    @Override()
    public final DataFrame<R,C> applyDoubles(ToDoubleFunction<DataFrameValue<R,C>> mapper) {
        if (parallel && colCount() > 0) {
            final int toIndex = rowCount() * colCount() - 1;
            final int threshold = (rowCount() * colCount()) / Runtime.getRuntime().availableProcessors();
            final ApplyDoubles action = new ApplyDoubles(0, toIndex, threshold, mapper);
            ForkJoinPool.commonPool().invoke(action);
        } else if (colCount() > 0) {
            final int toIndex = rowCount() * colCount() - 1;
            final int threshold = Integer.MAX_VALUE;
            final ApplyDoubles action = new ApplyDoubles(0, toIndex, threshold, mapper);
            action.compute();
        }
        return this;
    }


    @Override()
    public final DataFrame<R,C> applyValues(Function<DataFrameValue<R,C>,?> mapper) {
        if (parallel && colCount() > 0) {
            final int toIndex = rowCount() * colCount() - 1;
            final int threshold = (rowCount() * colCount()) / Runtime.getRuntime().availableProcessors();
            final ApplyValues action = new ApplyValues(0, toIndex, threshold, mapper);
            ForkJoinPool.commonPool().invoke(action);
        } else if (colCount() > 0) {
            final int toIndex = rowCount() * colCount() - 1;
            final int threshold = Integer.MAX_VALUE;
            final ApplyValues action = new ApplyValues(0, toIndex, threshold, mapper);
            action.compute();
        }
        return this;
    }


    @Override()
    public DataFrame<R,C> sign() throws DataFrameException {
        final int rowCount = rowCount();
        final int colCount = colCount();
        final Index<R> rowIndex = Index.of(rows().keyArray());
        final Index<C> colIndex = Index.of(cols().keyArray());
        final XDataFrame<R,C> result = (XDataFrame<R,C>)DataFrame.ofInts(rowIndex, colIndex);
        for (int i=0; i<rowCount; ++i) {
            for (int j=0; j<colCount; ++j) {
                final double value = data.getDouble(i, j);
                result.data().setInt(i, j, 0);
                if (value > 0d) {
                    result.data().setInt(i, j, 1);
                } else if (value < 0d) {
                    result.data().setInt(i, j, -1);
                }
            }
        }
        return result;
    }


    @Override()
    public DataFrameOutput<R,C> out() {
        return new XDataFrameOutput<>(this, new Formats());
    }


    @Override()
    public DataFrameFill fill() {
        return new XDataFrameFill<>(this);
    }


    @Override()
    public final Stats<Double> stats() {
        return new XDataFrameStats<>(true, this);
    }


    @Override
    public DataFrame<C, R> transpose() {
        return new XDataFrame<>(data.transpose(), isParallel());
    }


    @Override
    public Decomposition decomp() {
        return algebra().decomp();
    }


    @Override
    public DataFrame<Integer,Integer> inverse() throws DataFrameException {
        return algebra().inverse();
    }


    @Override
    public DataFrame<Integer,Integer> solve(DataFrame<?,?> rhs) throws DataFrameException {
        return algebra().solve(rhs);
    }


    @Override
    public DataFrame<R,C> plus(Number scalar) throws DataFrameException {
        return algebra().plus(scalar);
    }


    @Override
    public DataFrame<R,C> plus(DataFrame<?, ?> other) throws DataFrameException {
        return algebra().plus(other);
    }


    @Override
    public DataFrame<R,C> minus(Number scalar) throws DataFrameException {
        return algebra().minus(scalar);
    }


    @Override
    public DataFrame<R,C> minus(DataFrame<?,?> other) throws DataFrameException {
        return algebra().minus(other);
    }


    @Override
    public DataFrame<R,C> times(Number scalar) throws DataFrameException {
        return algebra().times(scalar);
    }


    @Override
    public DataFrame<R,C> times(DataFrame<?,?> other) throws DataFrameException {
        return algebra().times(other);
    }


    @Override
    public <X,Y> DataFrame<R,Y> dot(DataFrame<X,Y> right) throws DataFrameException {
        return algebra().dot(right);
    }


    @Override
    public DataFrame<R,C> divide(Number scalar) throws DataFrameException {
        return algebra().divide(scalar);
    }


    @Override
    public DataFrame<R,C> divide(DataFrame<?,?> other) throws DataFrameException {
        return algebra().divide(other);
    }


    @Override()
    public final DataFrameRank<R,C> rank() {
        return new XDataFrameRank<>(this);
    }


    @Override()
    public DataFrameEvents events() {
        return events;
    }


    @Override()
    public DataFrameWrite<R,C> write() {
        return new XDataFrameWrite<>(this);
    }


    @Override()
    public DataFrameExport export() {
        return new XDataFrameExport<>(this);
    }


    @Override
    public DataFrameCap<R,C> cap(boolean inPlace) {
        return new XDataFrameCap<>(inPlace, this);
    }


    @Override()
    public DataFrameCalculate<R,C> calc() {
        return new XDataFrameCalculate<>(this);
    }


    @Override()
    public DataFramePCA<R,C> pca() {
        return new XDataFramePCA<>(this);
    }


    @Override
    public DataFrameSmooth<R, C> smooth(boolean inPlace) {
        return new XDataFrameSmooth<>(inPlace, this);
    }


    @Override
    public DataFrameRegression<R, C> regress() {
        return new XDataFrameRegression<>(this);
    }


    @Override()
    public final DataFrame<R,C> addAll(DataFrame<R,C> other) {
        try {
            rows().addAll(other);
            cols().addAll(other);
            return this;
        } catch (Throwable t) {
            throw new DataFrameException("Failed to add rows/columns from other DataFrame: " + t.getMessage(), t);
        }
    }


    @Override()
    public final DataFrame<R,C> update(DataFrame<R,C> update, boolean addRows, boolean addColumns) throws DataFrameException {
        try {
            final XDataFrame<R,C> other = (XDataFrame<R,C>)update;
            if (addRows) rows().addAll(update.rows().keyArray());
            if (addColumns) cols().addAll(update);
            final Array<R> rowKeys = rowKeys().intersect(other.rowKeys());
            final Array<C> colKeys = colKeys().intersect(other.colKeys());
            final int[] sourceRows = other.rowKeys().ordinals(rowKeys).toArray();
            final int[] sourceCols = other.colKeys().ordinals(colKeys).toArray();
            final int[] targetRows = this.rowKeys().ordinals(rowKeys).toArray();
            final int[] targetCols = this.colKeys().ordinals(colKeys).toArray();
            for (int i=0; i<sourceRows.length; ++i) {
                for (int j=0; j<sourceCols.length; ++j) {
                    final Object value = update.data().getValue(sourceRows[i], sourceCols[j]);
                    this.data.setValue(targetRows[i], targetCols[j], value);
                }
            }
            return this;
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame data bulk update failed: " + t.getMessage(), t);
        }
    }


    @Override()
    public Iterator<DataFrameValue<R,C>> iterator() {
        final DataFrameCursor<R,C> value = cursor();
        return new Iterator<DataFrameValue<R,C>>() {
            private int rowIndex = 0;
            private int colIndex = 0;
            @Override
            public DataFrameValue<R,C> next() {
                value.atOrdinals(rowIndex++, colIndex);
                if (rowIndex == rowCount()) {
                    rowIndex = 0;
                    colIndex++;
                }
                return value;
            }
            @Override
            public boolean hasNext() {
                return rowIndex < rowCount() && colIndex < colCount();
            }
        };
    }


    @Override()
    public Iterator<DataFrameValue<R,C>> iterator(Predicate<DataFrameValue<R,C>> predicate) {
        final DataFrameCursor<R,C> value = cursor();
        return new Iterator<DataFrameValue<R,C>>() {
            private int rowIndex = 0;
            private int colIndex = 0;
            @Override
            public DataFrameValue<R,C> next() {
                value.atOrdinals(rowIndex++, colIndex);
                if (rowIndex == rowCount()) {
                    rowIndex = 0;
                    colIndex++;
                }
                return value;
            }
            @Override
            public boolean hasNext() {
                while (rowIndex < rowCount() && colIndex < colCount()) {
                    value.atOrdinals(rowIndex, colIndex);
                    if (predicate == null || predicate.test(value)) {
                        return true;
                    } else {
                        ++rowIndex;
                        if (rowIndex == rowCount()) {
                            rowIndex = 0;
                            colIndex++;
                        }
                    }
                }
                return false;
            }
        };
    }


    @Override()
    public final DataFrame<R, C> head(int count) {
        final IntStream indexes = IntStream.range(0, Math.min(count, rowCount()));
        final Array<R> keys = indexes.mapToObj(i -> rows().key(i)).collect(ArrayUtils.toArray());
        final Index<R> newRowAxis = rowKeys().filter(keys);
        final Index<C> newColAxis = colKeys();
        final XDataFrameContent<R,C> newContents = data.filter(newRowAxis, newColAxis);
        return new XDataFrame<>(newContents, parallel);
    }


    @Override()
    public final DataFrame<R, C> tail(int count) {
        final IntStream indexes = IntStream.range(Math.max(0, rowCount() - count), rowCount());
        final Array<R> keys = indexes.mapToObj(i -> rows().key(i)).collect(ArrayUtils.toArray());
        final Index<R> newRowAxis = rowKeys().filter(keys);
        final Index<C> newColAxis = colKeys();
        final XDataFrameContent<R,C> newContents = data.filter(newRowAxis, newColAxis);
        return new XDataFrame<>(newContents, parallel);
    }


    @Override()
    public final DataFrame<R,C> left(int count) {
        final Array<C> colKeys = colKeys().toArray(0, Math.min(colCount(), count));
        final Index<C> newColAxis = colKeys().filter(colKeys);
        final XDataFrameContent<R,C> newContents = data.filter(rowKeys(), newColAxis);
        return new XDataFrame<>(newContents, parallel);
    }


    @Override()
    public final DataFrame<R,C> right(int count) {
        final Array<C> colKeys = colKeys().toArray(Math.max(0, colCount() - count), colCount());
        final Index<C> newColAxis = colKeys().filter(colKeys);
        final XDataFrameContent<R,C> newContents = data.filter(rowKeys(), newColAxis);
        return new XDataFrame<>(newContents, parallel);
    }


    @Override()
    public final DataFrame<R,C> select(Iterable<R> rowKeys, Iterable<C> colKeys) {
        final Index<R> newRowAxis = rowKeys().filter(rowKeys);
        final Index<C> newColAxis = colKeys().filter(colKeys);
        final XDataFrameContent<R,C> newContents = data.filter(newRowAxis, newColAxis);
        return new XDataFrame<>(newContents, parallel);
    }


    @Override()
    public final DataFrame<R,C> select(Predicate<DataFrameRow<R,C>> rowPredicate, Predicate<DataFrameColumn<R,C>> colPredicate) {
        final SelectRows selectRows = new SelectRows(0, rowCount()-1, rowPredicate);
        final SelectColumns selectCols = new SelectColumns(0, colCount()-1, colPredicate);
        final Array<R> rowKeys = isParallel() ? ForkJoinPool.commonPool().invoke(selectRows) : selectRows.compute();
        final Array<C> colKeys = isParallel() ? ForkJoinPool.commonPool().invoke(selectCols) : selectCols.compute();
        final Index<R> newRowAxis = rowKeys().filter(rowKeys);
        final Index<C> newColAxis = colKeys().filter(colKeys);
        final XDataFrameContent<R,C> newContents = data.filter(newRowAxis, newColAxis);
        return new XDataFrame<>(newContents, parallel);
    }


    @Override
    public DataFrame<R, C> mapToBooleans(ToBooleanFunction<DataFrameValue<R, C>> mapper) {
        final Array<R> rowKeys = rows().keyArray();
        final Array<C> colKeys = cols().keyArray();
        final XDataFrame<R,C> result = (XDataFrame<R,C>)DataFrame.ofBooleans(rowKeys, colKeys);
        this.forEachValue(v -> {
            final boolean value = mapper.applyAsBoolean(v);
            final int rowOrdinal = v.rowOrdinal();
            final int colOrdinal = v.colOrdinal();
            result.content().setBoolean(rowOrdinal, colOrdinal, value);
        });
        return result;
    }


    @Override
    public DataFrame<R, C> mapToInts(ToIntFunction<DataFrameValue<R, C>> mapper) {
        final Array<R> rowKeys = rows().keyArray();
        final Array<C> colKeys = cols().keyArray();
        final XDataFrame<R,C> result = (XDataFrame<R,C>)DataFrame.ofInts(rowKeys, colKeys);
        this.forEachValue(v -> {
            final int value = mapper.applyAsInt(v);
            final int rowOrdinal = v.rowOrdinal();
            final int colOrdinal = v.colOrdinal();
            result.content().setInt(rowOrdinal, colOrdinal, value);
        });
        return result;
    }


    @Override
    public DataFrame<R, C> mapToLongs(ToLongFunction<DataFrameValue<R, C>> mapper) {
        final Array<R> rowKeys = rows().keyArray();
        final Array<C> colKeys = cols().keyArray();
        final XDataFrame<R,C> result = (XDataFrame<R,C>)DataFrame.ofLongs(rowKeys, colKeys);
        this.forEachValue(v -> {
            final long value = mapper.applyAsLong(v);
            final int rowOrdinal = v.rowOrdinal();
            final int colOrdinal = v.colOrdinal();
            result.content().setLong(rowOrdinal, colOrdinal, value);
        });
        return result;
    }


    @Override()
    public final DataFrame<R,C> mapToDoubles(ToDoubleFunction<DataFrameValue<R,C>> mapper) {
        final Array<R> rowKeys = rows().keyArray();
        final Array<C> colKeys = cols().keyArray();
        final XDataFrame<R,C> result = (XDataFrame<R,C>)DataFrame.ofDoubles(rowKeys, colKeys);
        this.forEachValue(v -> {
            final double value = mapper.applyAsDouble(v);
            final int rowOrdinal = v.rowOrdinal();
            final int colOrdinal = v.colOrdinal();
            result.content().setDouble(rowOrdinal, colOrdinal, value);
        });
        return result;
    }


    @Override
    public <T> DataFrame<R,C> mapToObjects(Class<T> type, Function<DataFrameValue<R,C>,T> mapper) {
        final Array<R> rowKeys = rows().keyArray();
        final Class<C> colKeyType = cols().keyType();
        final XDataFrame<R,C> result = (XDataFrame<R,C>)DataFrame.of(rowKeys, colKeyType, columns -> {
           cols().keys().forEach(colKey -> {
               columns.add(colKey, type);
           });
        });
        this.forEachValue(v -> {
            final T value = mapper.apply(v);
            final int rowOrdinal = v.rowOrdinal();
            final int colOrdinal = v.colOrdinal();
            result.content().setValue(rowOrdinal, colOrdinal, value);
        });
        return result;
    }


    @Override
    public DataFrame<R,C> mapToBooleans(C colKey, ToBooleanFunction<DataFrameValue<R,C>> mapper) {
        return new XDataFrame<>(content().mapToBooleans(this, colKey, mapper), isParallel());
    }


    @Override
    public DataFrame<R,C> mapToInts(C colKey, ToIntFunction<DataFrameValue<R,C>> mapper) {
        return new XDataFrame<>(content().mapToInts(this, colKey, mapper), isParallel());
    }


    @Override
    public DataFrame<R,C> mapToLongs(C colKey, ToLongFunction<DataFrameValue<R,C>> mapper) {
        return new XDataFrame<>(content().mapToLongs(this, colKey, mapper), isParallel());
    }


    @Override
    public DataFrame<R,C> mapToDoubles(C colKey, ToDoubleFunction<DataFrameValue<R,C>> mapper) {
        return new XDataFrame<>(content().mapToDoubles(this, colKey, mapper), isParallel());
    }


    @Override
    public <T> DataFrame<R,C> mapToObjects(C colKey, Class<T> type, Function<DataFrameValue<R,C>,T> mapper) {
        return new XDataFrame<>(content().mapToObjects(this, colKey, type, mapper), isParallel());
    }


    @Override()
    public Stream<DataFrameValue<R,C>> values() {
        final int valueCount = rowCount() * colCount();
        final int splitThreshold = valueCount / Runtime.getRuntime().availableProcessors();
        return StreamSupport.stream(new DataFrameValueSpliterator<>(0, valueCount-1, rowCount(), splitThreshold), isParallel());
    }


    @Override()
    public final boolean equals(Object object) {
        if (!(object instanceof DataFrame)) {
            return false;
        } else {
            final int rowCount = rowCount();
            final int colCount = colCount();
            final DataFrame other = (DataFrame)object;
            if (other.rowCount() != rowCount || other.colCount() != colCount) {
                return false;
            } else {
                for (int i=0; i<rowCount; ++i) {
                    final Object rowKey1 = rows().key(i);
                    final Object rowKey2 = other.rows().key(i);
                    if (!rowKey1.equals(rowKey2)) {
                        return false;
                    }
                }
                for (int j=0; j<colCount; ++j) {
                    final Object colKey1 = cols().key(j);
                    final Object colKey2 = other.cols().key(j);
                    if (!colKey1.equals(colKey2)) {
                        return false;
                    }
                }
                for (int i=0; i<rowCount; ++i) {
                    for (int j=0; j<colCount; ++j) {
                        final Object value1 = data.getValue(i, j);
                        final Object value2 = other.data().getValue(i, j);
                        if (value1 == null && value2 != null) {
                            return false;
                        } else if (value1 != null && !value1.equals(value2)) {
                            return false;
                        }
                    }
                }
                return true;
            }
        }
    }


    @Override()
    public String toString() {
        final int rowCount = rows.count();
        final int colCount = cols.count();
        final StringBuilder text = new StringBuilder();
        text.append("DataFrame[").append(rowCount).append("x").append(colCount).append("]");
        if (rowCount > 0) {
            text.append(" rows=[");
            text.append(rows.firstKey().orElse(null));
            text.append("...");
            text.append(rows.lastKey().orElse(null));
            text.append("]");
        }
        if (colCount > 0) {
            text.append(", columns=[");
            text.append(cols.firstKey().orElse(null));
            text.append("...");
            text.append(cols.lastKey().orElse(null));
            text.append("]");
        }
        return text.toString();
    }


    /**
     * Custom object serialization method for improved performance
     * @param os    the output stream
     * @throws IOException  if write fails
     */
    private void writeObject(ObjectOutputStream os) throws IOException {
        os.writeObject(data);
    }


    /**
     * Custom object serialization method for improved performance
     * @param is    the input stream
     * @throws IOException  if read fails
     * @throws ClassNotFoundException   if read fails
     */
    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
        this.data = (XDataFrameContent)is.readObject();
        this.events = new XDataFrameEvents();
        this.rows = new XDataFrameRows<>(this, false);
        this.cols = new XDataFrameColumns<>(this, false);
    }


    /**
     * A RecursiveAction to apply booleans in a DataFrame
     */
    private class ApplyBooleans extends RecursiveAction {

        private int from;
        private int to;
        private int threshold;
        private ToBooleanFunction<DataFrameValue<R,C>> mapper;

        /**
         * Constructor
         * @param from      the from column index in view space
         * @param to        the to column index in view space
         * @param threshold the threshold to trigger parallelism
         * @param mapper    the mapper function
         */
        ApplyBooleans(int from, int to, int threshold, ToBooleanFunction<DataFrameValue<R,C>> mapper) {
            this.from = from;
            this.to = to;
            this.threshold = threshold;
            this.mapper = mapper;
        }

        @Override
        protected void compute() {
            final int count = (to - from) + 1;
            if (count > threshold) {
                final int midPoint = from + ((to - from) / 2);
                invokeAll(
                    new ApplyBooleans(from, midPoint, threshold, mapper),
                    new ApplyBooleans(midPoint+1, to, threshold, mapper)
                );
            } else {
                final int rowCount = rowCount();
                final DataFrameCursor<R,C> value = cursor();
                for (int index=from; index<=to; ++index) {
                    final int rowOrdinal = index % rowCount;
                    final int colOrdinal = index / rowCount;
                    value.atOrdinals(rowOrdinal, colOrdinal);
                    final boolean result = mapper.applyAsBoolean(value);
                    value.setBoolean(result);
                }
            }
        }
    }


    /**
     * A RecursiveAction to apply ints in a DataFrame
     */
    private class ApplyInts extends RecursiveAction {

        private int from;
        private int to;
        private int threshold;
        private ToIntFunction<DataFrameValue<R,C>> mapper;

        /**
         * Constructor
         * @param from      the from column index in view space
         * @param to        the to column index in view space
         * @param threshold the threshold to trigger parallelism
         * @param mapper    the mapper function
         */
        ApplyInts(int from, int to, int threshold, ToIntFunction<DataFrameValue<R,C>> mapper) {
            this.from = from;
            this.to = to;
            this.threshold = threshold;
            this.mapper = mapper;
        }

        @Override
        protected void compute() {
            final int count = (to - from) + 1;
            if (count > threshold) {
                final int midPoint = from + ((to - from) / 2);
                invokeAll(
                    new ApplyInts(from, midPoint, threshold, mapper),
                    new ApplyInts(midPoint+1, to, threshold, mapper)
                );
            } else {
                final int rowCount = rowCount();
                final DataFrameCursor<R,C> value = cursor();
                for (int index=from; index<=to; ++index) {
                    final int rowOrdinal = index % rowCount;
                    final int colOrdinal = index / rowCount;
                    value.atOrdinals(rowOrdinal, colOrdinal);
                    final int result = mapper.applyAsInt(value);
                    value.setInt(result);
                }
            }
        }
    }


    /**
     * A RecursiveAction to apply longs in a DataFrame
     */
    private class ApplyLongs extends RecursiveAction {

        private int from;
        private int to;
        private int threshold;
        private ToLongFunction<DataFrameValue<R,C>> mapper;

        /**
         * Constructor
         * @param from      the from column index in view space
         * @param to        the to column index in view space
         * @param threshold the threshold to trigger parallelism
         * @param mapper    the mapper function
         */
        ApplyLongs(int from, int to, int threshold, ToLongFunction<DataFrameValue<R,C>> mapper) {
            this.from = from;
            this.to = to;
            this.threshold = threshold;
            this.mapper = mapper;
        }

        @Override
        protected void compute() {
            final int count = (to - from) + 1;
            if (count > threshold) {
                final int midPoint = from + ((to - from) / 2);
                invokeAll(
                    new ApplyLongs(from, midPoint, threshold, mapper),
                    new ApplyLongs(midPoint+1, to, threshold, mapper)
                );
            } else {
                final int rowCount = rowCount();
                final DataFrameCursor<R,C> value = cursor();
                for (int index=from; index<=to; ++index) {
                    final int rowOrdinal = index % rowCount;
                    final int colOrdinal = index / rowCount;
                    value.atOrdinals(rowOrdinal, colOrdinal);
                    final long result = mapper.applyAsLong(value);
                    value.setLong(result);
                }
            }
        }
    }


    /**
     * A RecursiveAction to apply doubles in a DataFrame
     */
    private class ApplyDoubles extends RecursiveAction {

        private int from;
        private int to;
        private int threshold;
        private ToDoubleFunction<DataFrameValue<R,C>> mapper;

        /**
         * Constructor
         * @param from      the from coordinate
         * @param to        the to coordinate
         * @param threshold the threshold to trigger parallelism
         * @param mapper    the mapper function
         */
        ApplyDoubles(int from, int to, int threshold, ToDoubleFunction<DataFrameValue<R,C>> mapper) {
            this.from = from;
            this.to = to;
            this.threshold = threshold;
            this.mapper = mapper;
        }

        @Override
        protected void compute() {
            final int count = (to - from) + 1;
            if (count > threshold) {
                final int midPoint = from + ((to - from) / 2);
                invokeAll(
                    new ApplyDoubles(from, midPoint, threshold, mapper),
                    new ApplyDoubles(midPoint+1, to, threshold, mapper)
                );
            } else {
                final int rowCount = rowCount();
                final DataFrameCursor<R,C> value = cursor();
                for (int index=from; index<=to; ++index) {
                    final int rowOrdinal = index % rowCount;
                    final int colOrdinal = index / rowCount;
                    value.atOrdinals(rowOrdinal, colOrdinal);
                    final double result = mapper.applyAsDouble(value);
                    value.setDouble(result);
                }
            }
        }
    }


    /**
     * A RecursiveAction to apply objects in a DataFrame
     */
    private class ApplyValues extends RecursiveAction {

        private int from;
        private int to;
        private int threshold;
        private Function<DataFrameValue<R,C>,?> mapper;

        /**
         * Constructor
         * @param from      the from column index in view space
         * @param to        the to column index in view space
         * @param threshold the threshold to trigger parallelism
         * @param mapper    the mapper function
         */
        ApplyValues(int from, int to, int threshold, Function<DataFrameValue<R,C>,?> mapper) {
            this.from = from;
            this.to = to;
            this.threshold = threshold;
            this.mapper = mapper;
        }

        @Override
        protected void compute() {
            final int count = (to - from) + 1;
            if (count > threshold) {
                final int midPoint = from + ((to - from) / 2);
                invokeAll(
                    new ApplyValues(from, midPoint, threshold, mapper),
                    new ApplyValues(midPoint+1, to, threshold, mapper)
                );
            } else {
                final int rowCount = rowCount();
                final DataFrameCursor<R,C> value = cursor();
                for (int index=from; index<=to; ++index) {
                    final int rowOrdinal = index % rowCount;
                    final int colOrdinal = index / rowCount;
                    value.atOrdinals(rowOrdinal, colOrdinal);
                    final Object result = mapper.apply(value);
                    value.setValue(result);
                }
            }
        }
    }


    /**
     * A RecursiveAction to iterate through all values in a DataFrame
     */
    private class ForEachValue extends RecursiveAction {

        private int from;
        private int to;
        private int threshold;
        private Consumer<DataFrameValue<R,C>> consumer;

        /**
         * Constructor
         * @param from      the from column index in view space
         * @param to        the to column index in view space
         * @param threshold the threshold to trigger parallelism
         * @param consumer    the mapper function
         */
        ForEachValue(int from, int to, int threshold, Consumer<DataFrameValue<R,C>> consumer) {
            this.from = from;
            this.to = to;
            this.threshold = threshold;
            this.consumer = consumer;
        }

        @Override
        protected void compute() {
            final int count = (to - from) + 1;
            if (count > threshold) {
                final int midPoint = from + ((to - from) / 2);
                invokeAll(
                    new ForEachValue(from, midPoint, threshold, consumer),
                    new ForEachValue(midPoint+1, to, threshold, consumer)
                );
            } else {
                final int rowCount = rowCount();
                final DataFrameCursor<R,C> value = cursor();
                for (int index=from; index<=to; ++index) {
                    final int rowOrdinal = index % rowCount;
                    final int colOrdinal = index / rowCount;
                    value.atOrdinals(rowOrdinal, colOrdinal);
                    consumer.accept(value);
                }
            }
        }
    }



    /**
     * A Spliterator implementation to iterate over all values in a DataFrame.
     * @param <X>   the row key type
     * @param <Y>   the column key type
     */
    private class DataFrameValueSpliterator<X,Y> implements Spliterator<DataFrameValue<X,Y>> {

        private int position;
        private int start;
        private int end;
        private int rowCount;
        private int splitThreshold;
        private DataFrameCursor<X,Y> value;

        /**
         * Constructor
         * @param start             the start ordinal
         * @param end               the end ordinal
         * @param rowCount          the row count of frame when Spliterator was originally created
         * @param splitThreshold    the split threshold
         */
        @SuppressWarnings("unchecked")
        private DataFrameValueSpliterator(int start, int end, int rowCount, int splitThreshold) {
            Asserts.check(start <= end, "The from ordinal must be <= the to oridinal");
            Asserts.check(splitThreshold > 0, "The split threshold must be > 0");
            this.position = start;
            this.start = start;
            this.end = end;
            this.rowCount = rowCount;
            this.splitThreshold = splitThreshold;
            this.value = (DataFrameCursor<X,Y>)cursor();
        }

        @Override
        public boolean tryAdvance(Consumer<? super DataFrameValue<X,Y>> action) {
            Asserts.check(action != null, "The consumer action cannot be null");
            if (position <= end) {
                final int rowOrdinal = position % rowCount;
                final int colOrdinal = position / rowCount;
                this.value.atOrdinals(rowOrdinal, colOrdinal);
                this.position++;
                action.accept(value);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public Spliterator<DataFrameValue<X,Y>> trySplit() {
            if (estimateSize() < splitThreshold) {
                return null;
            } else {
                final int newStart = start;
                final int halfSize = (end - start) / 2;
                final int newEnd = newStart + halfSize;
                this.start = newEnd + 1;
                this.position = start;
                return new DataFrameValueSpliterator<>(newStart, newEnd, rowCount, splitThreshold);
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
     * A RecursiveTask to select row keys that match a user provided predicate
     */
    private class SelectRows extends RecursiveTask<Array<R>> {

        private int from;
        private int to;
        private int threshold;
        private Predicate<DataFrameRow<R,C>> predicate;

        /**
         * Constructor
         * @param from      the from ordinal
         * @param to        the to row index in view space
         * @param predicate the predicate to match rows
         */
        SelectRows(int from, int to, Predicate<DataFrameRow<R,C>> predicate) {
            this.from = from;
            this.to = to;
            this.predicate = predicate;
            this.threshold = Integer.MAX_VALUE;
            if (isParallel()) {
                this.threshold = DataFrameOptions.getRowSplitThreshold(XDataFrame.this);
            }
        }

        @Override
        protected Array<R> compute() {
            final int count = to - from + 1;
            final Class<R> keyType = rows().keyType();
            if (count > threshold) {
                return split();
            } else {
                final int rowCount = rowCount();
                final XDataFrameRow<R,C> row = new XDataFrameRow<>(XDataFrame.this, false);
                final ArrayBuilder<R> builder = ArrayBuilder.of(rowCount > 0 ? rowCount : 10, keyType);
                for (int ordinal=from; ordinal<=to; ++ordinal) {
                    row.moveTo(ordinal);
                    if (predicate.test(row)) {
                        builder.add(row.key());
                    }
                }
                return builder.toArray();
            }
        }

        /**
         * Splits this task into two sub-tasks and executes them in parallel
         * @return  the join results from the two sub-tasks
         */
        private Array<R> split() {
            final int splitCount = (to - from) / 2;
            final int midPoint = from + splitCount;
            final SelectRows left  = new SelectRows(from, midPoint, predicate);
            final SelectRows right = new SelectRows(midPoint + 1, to, predicate);
            left.fork();
            final Array<R> rightAns = right.compute();
            final Array<R> leftAns  = left.join();
            final int size = Math.max(rightAns.length() + leftAns.length(), 10);
            final Class<R> rowKeyType = rows().keyType();
            final ArrayBuilder<R> builder = ArrayBuilder.of(size, rowKeyType);
            builder.addAll(leftAns);
            builder.addAll(rightAns);
            return builder.toArray();
        }
    }


    /**
     * A RecursiveTask to select column keys that match a user provided predicate
     */
    private class SelectColumns extends RecursiveTask<Array<C>> {

        private int from;
        private int to;
        private int threshold;
        private Predicate<DataFrameColumn<R,C>> predicate;

        /**
         * Constructor
         * @param from      the from ordinal
         * @param to        the to col ordinal
         * @param predicate the predicate to match columns
         */
        SelectColumns(int from, int to, Predicate<DataFrameColumn<R,C>> predicate) {
            this.from = from;
            this.to = to;
            this.predicate = predicate;
            this.threshold = Integer.MAX_VALUE;
            if (isParallel()) {
                this.threshold = DataFrameOptions.getColumnSplitThreshold(XDataFrame.this);
            }
        }

        @Override
        protected Array<C> compute() {
            final int count = to - from + 1;
            final Class<C> keyType = cols().keyType();
            if (count > threshold) {
                return split();
            } else {
                final int colCount = colCount();
                final XDataFrameColumn<R,C> column = new XDataFrameColumn<>(XDataFrame.this, false);
                final ArrayBuilder<C> builder = ArrayBuilder.of(colCount > 0 ? colCount : 10, keyType);
                for (int ordinal=from; ordinal<=to; ++ordinal) {
                    column.moveTo(ordinal);
                    if (predicate.test(column)) {
                        builder.add(column.key());
                    }
                }
                return builder.toArray();
            }
        }

        /**
         * Splits this task into two sub-tasks and executes them in parallel
         * @return  the join results from the two sub-tasks
         */
        private Array<C> split() {
            final int splitCount = (to - from) / 2;
            final int midPoint = from + splitCount;
            final SelectColumns left  = new SelectColumns(from, midPoint, predicate);
            final SelectColumns right = new SelectColumns(midPoint + 1, to, predicate);
            left.fork();
            final Array<C> rightAns = right.compute();
            final Array<C> leftAns  = left.join();
            final int size = Math.max(rightAns.length() + leftAns.length(), 10);
            final ArrayBuilder<C> builder = ArrayBuilder.of(size, cols().keyType());
            builder.addAll(leftAns);
            builder.addAll(rightAns);
            return builder.toArray();
        }
    }

    /**
     * A task to find the min value in the DataFrame
     */
    private class MinMaxValueTask extends RecursiveTask<Optional<DataFrameValue<R,C>>> {

        private int offset;
        private int length;
        private boolean min;
        private int threshold = Integer.MAX_VALUE;
        private Predicate<DataFrameValue<R,C>> predicate;

        /**
         * Constructor
         * @param offset    the offset ordinal
         * @param length    the number of items for this task
         * @param min       if true, task finds the min value, else the max value
         * @param predicate the predicate to filter on values
         */
        MinMaxValueTask(int offset, int length, boolean min, Predicate<DataFrameValue<R,C>> predicate) {
            this.offset = offset;
            this.length = length;
            this.min = min;
            this.predicate = predicate;
            if (isParallel() && rowCount() > colCount()) {
                this.threshold = DataFrameOptions.getRowSplitThreshold(XDataFrame.this);
            } else if (isParallel()) {
                this.threshold = DataFrameOptions.getColumnSplitThreshold(XDataFrame.this);
            }
        }

        @Override
        protected Optional<DataFrameValue<R,C>> compute() {
            if (length > threshold) {
                return split();
            } else if (rowCount() > colCount()) {
                return initial().map(result -> {
                    DataFrameCursor<R,C> value = cursor();
                    final int rowStart = result.rowOrdinal() - offset;
                    for (int i=rowStart; i<length; ++i) {
                        value.atRowOrdinal(offset + i);
                        final int colStart = i == rowStart ? result.colOrdinal() : 0;
                        for (int j=colStart; j<colCount(); ++j) {
                            value.atColOrdinal(j);
                            if (predicate.test(value)) {
                                if (min && value.compareTo(result) < 0) {
                                    result.atOrdinals(value.rowOrdinal(), value.colOrdinal());
                                } else if (!min && value.compareTo(result) > 0) {
                                    result.atOrdinals(value.rowOrdinal(), value.colOrdinal());
                                }
                            }
                        }
                    }
                    return (DataFrameValue<R,C>)result;
                });
            } else {
                return initial().map(result -> {
                    DataFrameCursor<R,C> value = cursor();
                    final int colStart = result.colOrdinal() - offset;
                    for (int i=colStart; i<length; ++i) {
                        value.atColOrdinal(offset + i);
                        final int rowStart = i == colStart ? result.rowOrdinal() : 0;
                        for (int j=rowStart; j<rowCount(); ++j) {
                            value.atRowOrdinal(j);
                            if (predicate.test(value)) {
                                if (min && value.compareTo(result) < 0) {
                                    result.atOrdinals(value.rowOrdinal(), value.colOrdinal());
                                } else if (!min && value.compareTo(result) > 0) {
                                    result.atOrdinals(value.rowOrdinal(), value.colOrdinal());
                                }
                            }
                        }
                    }
                    return (DataFrameValue<R,C>)result;
                });
            }
        }

        /**
         * Initializes the result cursor to an appropriate starting point
         * @return      the initial result cursor to track min value
         */
        private Optional<DataFrameCursor<R,C>> initial() {
            DataFrameCursor<R,C> result = cursor();
            if (rowCount() > colCount()) {
                result.atOrdinals(offset, 0);
                for (int i=0; i<length; ++i) {
                    if (predicate.test(result)) break;
                    result.atRowOrdinal(offset + i);
                    for (int j=0; j<colCount(); ++j) {
                        result.atColOrdinal(j);
                        if (predicate.test(result)) {
                            break;
                        }
                    }
                }
            } else {
                result.atOrdinals(0, offset);
                for (int i=0; i<length; ++i) {
                    if (predicate.test(result)) break;
                    result.atColOrdinal(offset + i);
                    for (int j=0; j<rowCount(); ++j) {
                        result.atRowOrdinal(j);
                        if (predicate.test(result)) {
                            break;
                        }
                    }
                }
            }
            if (predicate.test(result)) {
                return Optional.of(result);
            } else {
                return Optional.empty();
            }
        }

        /**
         * Splits this task into two and computes min across the two tasks
         * @return      returns the min across the two split tasks
         */
        private Optional<DataFrameValue<R,C>> split() {
            final int splitLength = length / 2;
            final int midPoint = offset + splitLength;
            final MinMaxValueTask leftTask = new MinMaxValueTask(offset, splitLength, min, predicate);
            final MinMaxValueTask rightTask = new MinMaxValueTask(midPoint, length - splitLength, min, predicate);
            leftTask.fork();
            final Optional<DataFrameValue<R,C>> rightAns = rightTask.compute();
            final Optional<DataFrameValue<R,C>> leftAns = leftTask.join();
            if (leftAns.isPresent() && rightAns.isPresent()) {
                final DataFrameValue<R,C> left = leftAns.get();
                final DataFrameValue<R,C> right = rightAns.get();
                final int result = left.compareTo(right);
                return min ? result < 0 ? leftAns : rightAns : result > 0 ? leftAns : rightAns;
            } else {
                return leftAns.isPresent() ? leftAns : rightAns;
            }
        }
    }



    /**
     * A task to find the upper/lower bounds of the DataFrame
     */
    private class BoundsTask<V> extends RecursiveTask<Optional<Bounds<V>>> {

        private int offset;
        private int length;
        private int threshold = Integer.MAX_VALUE;
        private Predicate<DataFrameValue<R,C>> predicate;

        /**
         * Constructor
         * @param offset    the offset ordinal
         * @param length    the number of items for this task
         * @param predicate the predicate to filter on values
         */
        BoundsTask(int offset, int length, Predicate<DataFrameValue<R,C>> predicate) {
            this.offset = offset;
            this.length = length;
            this.predicate = predicate;
            if (isParallel() && rowCount() > colCount()) {
                this.threshold = DataFrameOptions.getRowSplitThreshold(XDataFrame.this);
            } else if (isParallel()) {
                this.threshold = DataFrameOptions.getColumnSplitThreshold(XDataFrame.this);
            }
        }

        @Override
        protected Optional<Bounds<V>> compute() {
            if (length > threshold) {
                return split();
            } else if (rowCount() > colCount()) {
                return initial().map(initial -> {
                    final DataFrameCursor<R,C> value = cursor();
                    final DataFrameCursor<R,C> min = initial.copy();
                    final DataFrameCursor<R,C> max = initial.copy();
                    final int rowStart = initial.rowOrdinal() - offset;
                    for (int i=rowStart; i<length; ++i) {
                        value.atRowOrdinal(offset + i);
                        final int colStart = i == rowStart ? initial.colOrdinal() : 0;
                        for (int j=colStart; j<colCount(); ++j) {
                            value.atColOrdinal(j);
                            if (predicate.test(value)) {
                                if (value.compareTo(min) < 0) {
                                    min.atOrdinals(value.rowOrdinal(), value.colOrdinal());
                                } else if (value.compareTo(max) > 0) {
                                    max.atOrdinals(value.rowOrdinal(), value.colOrdinal());
                                }
                            }
                        }
                    }
                    final V lower = min.getValue();
                    final V upper = max.getValue();
                    return Bounds.of(lower, upper);
                });
            } else {
                return initial().map(initial -> {
                    final DataFrameCursor<R,C> value = cursor();
                    final DataFrameCursor<R,C> min = initial.copy();
                    final DataFrameCursor<R,C> max = initial.copy();
                    final int colStart = initial.colOrdinal() - offset;
                    for (int i=colStart; i<length; ++i) {
                        value.atColOrdinal(offset + i);
                        final int rowStart = i == colStart ? initial.rowOrdinal() : 0;
                        for (int j=rowStart; j<rowCount(); ++j) {
                            value.atRowOrdinal(j);
                            if (predicate.test(value)) {
                                if (value.compareTo(min) < 0) {
                                    min.atOrdinals(value.rowOrdinal(), value.colOrdinal());
                                } else if (value.compareTo(max) > 0) {
                                    max.atOrdinals(value.rowOrdinal(), value.colOrdinal());
                                }
                            }
                        }
                    }
                    final V lower = min.getValue();
                    final V upper = max.getValue();
                    return Bounds.of(lower, upper);
                });
            }
        }

        /**
         * Initializes the result cursor to an appropriate starting point
         * @return      the initial result cursor to track min value
         */
        private Optional<DataFrameCursor<R,C>> initial() {
            DataFrameCursor<R,C> result = cursor();
            if (rowCount() > colCount()) {
                result.atOrdinals(offset, 0);
                for (int i=0; i<length; ++i) {
                    if (predicate.test(result)) break;
                    result.atRowOrdinal(offset + i);
                    for (int j=0; j<colCount(); ++j) {
                        result.atColOrdinal(j);
                        if (predicate.test(result)) {
                            break;
                        }
                    }
                }
            } else {
                result.atOrdinals(0, offset);
                for (int i=0; i<length; ++i) {
                    if (predicate.test(result)) break;
                    result.atColOrdinal(offset + i);
                    for (int j=0; j<rowCount(); ++j) {
                        result.atRowOrdinal(j);
                        if (predicate.test(result)) {
                            break;
                        }
                    }
                }
            }
            if (predicate.test(result)) {
                return Optional.of(result);
            } else {
                return Optional.empty();
            }
        }

        /**
         * Splits this task into two and computes min across the two tasks
         * @return      returns the min across the two split tasks
         */
        private Optional<Bounds<V>> split() {
            final int splitLength = length / 2;
            final int midPoint = offset + splitLength;
            final BoundsTask<V> leftTask = new BoundsTask<>(offset, splitLength, predicate);
            final BoundsTask<V> rightTask = new BoundsTask<>(midPoint, length - splitLength, predicate);
            leftTask.fork();
            final Optional<Bounds<V>> rightAns = rightTask.compute();
            final Optional<Bounds<V>> leftAns = leftTask.join();
            if (leftAns.isPresent() && rightAns.isPresent()) {
                final Bounds<V> left = leftAns.get();
                final Bounds<V> right = rightAns.get();
                return Optional.of(Bounds.ofAll(left, right));
            } else {
                return leftAns.isPresent() ? leftAns : rightAns;
            }
        }
    }



    /**
     * The double iterator over all numeric values in the frame
     */
    private class DoubleIterator implements PrimitiveIterator.OfDouble {

        private DataFrameValue<R,C> value;
        private Iterator<DataFrameValue<R,C>> iterator;

        /**
         * Constructor
         * @param iterator  the input iterator for this double iterator
         */
        DoubleIterator(Iterator<DataFrameValue<R,C>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public double nextDouble() {
            return value.getDouble();
        }

        @Override
        public boolean hasNext() {
            while (iterator.hasNext()) {
                this.value = iterator.next();
                if (value.isNumeric()) {
                    return true;
                }
            }
            return false;
        }
    }

}
