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

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayUtils;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameAxisStats;
import com.zavtech.morpheus.frame.DataFrameCursor;
import com.zavtech.morpheus.frame.DataFrameEvent;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameGrouping;
import com.zavtech.morpheus.frame.DataFrameOptions;
import com.zavtech.morpheus.frame.DataFrameRow;
import com.zavtech.morpheus.frame.DataFrameRows;
import com.zavtech.morpheus.frame.DataFrameValue;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.stats.StatType;
import com.zavtech.morpheus.stats.Stats;
import com.zavtech.morpheus.util.Parallel;

/**
 * The reference implementation of DataFrameOperator that operates in the row dimension of the DataFrame.
 *
 * @param <R>       the row key type
 * @param <C>       the column key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrameRows<R,C> extends XDataFrameAxisBase<R,C,R,C,DataFrameRow<R,C>,DataFrameRows<R,C>,DataFrameGrouping.Rows<R,C>> implements DataFrameRows<R,C> {

    /**
     * Constructor
     * @param frame     the frame to operate on
     * @param parallel  true for parallel implementation.
     */
    XDataFrameRows(XDataFrame<R,C> frame, boolean parallel) {
        super(frame, parallel, true);
    }


    @Override
    public boolean add(R key) throws DataFrameException {
        return add(key, null);
    }


    @Override
    public Array<R> addAll(Iterable<R> keys) {
        return addAll(keys, null);
    }


    @Override
    public final boolean add(R key, Function<DataFrameValue<R,C>,?> initials) {
        final XDataFrameContent<R,C> content = frame().content();
        final boolean added = content.addRow(key);
        final boolean ignoreDuplicates = DataFrameOptions.isIgnoreDuplicates();
        if (!added && !ignoreDuplicates) {
            throw new DataFrameException("Attempt to add duplicate row key: " + key);
        } else if (added) {
            final XDataFrame<R,C> frame = frame();
            final int ordinal = content.rowDim().getOrdinalForKey(key);
            if (initials != null) {
                final DataFrameCursor<R,C> value = frame.cursor().moveToRow(ordinal);
                for (int i=0; i<frame.colCount(); ++i) {
                    value.moveToColumn(i);
                    final Object result = initials.apply(value);
                    value.setValue(result);
                }
            }
            if (frame.events().isEnabled()) {
                final Array<R> keyList = Array.singleton(key);
                final DataFrameEvent event = DataFrameEvent.createRowAdd(frame, keyList);
                frame.events().fireDataFrameEvent(event);
            }
        }
        return added;
    }


    @Override
    public final Array<R> addAll(Iterable<R> keys, Function<DataFrameValue<R,C>,?> initials) {
        final XDataFrameContent<R,C> content = frame().content();
        final Array<R> added = content.addRows(keys);
        if (initials != null) {
            final DataFrameCursor<R,C> cursor = frame().cursor();
            added.forEach(rowKey -> {
                cursor.moveToRow(rowKey);
                for (int i=0; i<frame().colCount(); ++i) {
                    cursor.moveToColumn(i);
                    Object value = initials.apply(cursor);
                    cursor.setValue(value);
                }
            });
        }
        final DataFrame<R,C> frame = frame();
        if (added.length() > 0) {
            if (frame().events().isEnabled()) {
                final DataFrameEvent event = DataFrameEvent.createRowAdd(frame, added);
                frame.events().fireDataFrameEvent(event);
            }
        }
        return added;
    }


    @Override
    public final Array<R> addAll(DataFrame<R,C> other) {
        final XDataFrame<R,C> target = frame();
        final XDataFrame<R,C> source = (XDataFrame<R,C>)other;
        final Array<R> rowKeys = target.content().addRows(source.rowKeys());
        if (rowKeys.length() == 0) {
            return rowKeys;
        } else {
            final Array<C> colKeys = target.colKeys().intersect(source.colKeys());
            XDataFrameCopy.apply(source, target, rowKeys, colKeys);
            if (target.events().isEnabled()) {
                final DataFrameEvent<R,C> event = DataFrameEvent.createRowAdd(target, rowKeys);
                target.events().fireDataFrameEvent(event);
            }
            return rowKeys;
        }
    }


    @Override
    public final DataFrameRows<R,C> parallel() {
        return isParallel() ? this : new XDataFrameRows<>(frame(), true);
    }

    @Override
    public final DataFrameRows<R,C> sequential() {
        return !isParallel() ? this : new XDataFrameRows<>(frame(), false);
    }

    @Override
    public final DataFrameAxisStats<R,R,C,R,StatType> stats() {
        return new XDataFrameAxisStats<>(frame(), isParallel(), false);
    }

    @Override @Parallel
    public final DataFrame<R,C> sort(boolean ascending) {
        return XDataFrameSorter.sortRows(frame(), ascending, isParallel());
    }

    @Override  @Parallel
    public final DataFrame<R,C> sort(boolean ascending, C key) {
        return XDataFrameSorter.sortRows(frame(), key, ascending, isParallel());
    }

    @Override @Parallel
    public final DataFrame<R,C> sort(boolean ascending, List<C> keys) {
        return XDataFrameSorter.sortRows(frame(), keys, ascending, isParallel());
    }

    @Override @Parallel
    public final DataFrame<R,C> sort(Comparator<DataFrameRow<R,C>> comparator) {
        return XDataFrameSorter.sortRows(frame(), isParallel(), comparator);
    }

    @Override @Parallel
    public final DataFrame<R,C> demean(boolean inPlace) {
        if (!inPlace) {
            return frame().copy().rows().demean(true);
        } else {
            frame().rows().forEach(row -> {
                final double mean = row.stats().mean();
                row.applyDoubles(v -> v.getDouble() - mean);
            });
            return frame();
        }
    }

    @Override
    public final <X> DataFrame<X,C> mapKeys(Function<DataFrameRow<R,C>,X> mapper) {
        if (frame().rowKeys().isFilter()) {
            throw new DataFrameException("Row axis is immutable for this frame, call copy() first");
        } else {
            final XDataFrameRow<R,C> row = new XDataFrameRow<>(frame(), false);
            final Stream<X> newKeys = IntStream.range(0, count()).mapToObj(i -> mapper.apply(row.moveTo(i)));
            final Array<X> newArray = newKeys.collect(ArrayUtils.toArray(count()));
            final Index<X> newIndex = Index.of(newArray);
            return frame().withRowKeys(newIndex);
        }
    }

    @Override
    public final DataFrame<R,StatType> describe(StatType... stats) {
        final Array<R> rowKeys = filter(DataFrameRow::isNumeric).keyArray();
        final Array<StatType> statKeys = Array.of(StatType.class, stats);
        final DataFrame<R,StatType> result = DataFrame.ofDoubles(rowKeys, statKeys);
        this.filter(DataFrameRow::isNumeric).forEach(row -> {
            final R key = row.key();
            final Stats<Double> rowStats = row.stats();
            for (int j = 0; j < statKeys.length(); ++j) {
                final StatType stat = statKeys.getValue(j);
                final double value = stat.apply(rowStats);
                result.data().setDouble(key, j, value);
            }
        });
        return result;
    }

}
