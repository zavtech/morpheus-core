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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayType;
import com.zavtech.morpheus.array.ArrayBuilder;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameAxisStats;
import com.zavtech.morpheus.frame.DataFrameColumn;
import com.zavtech.morpheus.frame.DataFrameColumns;
import com.zavtech.morpheus.frame.DataFrameEvent;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameGrouping;
import com.zavtech.morpheus.frame.DataFrameOptions;
import com.zavtech.morpheus.frame.DataFrameValue;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.stats.StatType;
import com.zavtech.morpheus.stats.Stats;
import com.zavtech.morpheus.util.Asserts;
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
class XDataFrameColumns<R,C> extends XDataFrameAxisBase<C,R,R,C,DataFrameColumn<R,C>,DataFrameColumns<R,C>,DataFrameGrouping.Cols<R,C>> implements DataFrameColumns<R,C> {


    /**
     * Constructor
     * @param frame     the frame to operate on
     * @param parallel  true for parallel implementation
     */
    XDataFrameColumns(XDataFrame<R,C> frame, boolean parallel) {
        super(frame, parallel, false);
    }


    @Override
    public final DataFrameColumns<R,C> parallel() {
        return isParallel() ? this : new XDataFrameColumns<>(frame(), true);
    }


    @Override
    public final XDataFrameColumns<R,C> sequential() {
        return !isParallel() ? this : new XDataFrameColumns<>(frame(), false);
    }


    @Override
    public final DataFrameColumn<R,C> add(C key, Iterable<?> values) {
        return addColumns().andThen(notifyEvent()).andThen(keys -> frame().colAt(key)).apply(columnMap -> {
            if (!contains(key)) {
                columnMap.put(key, values);
            }
        });
    }


    @Override
    public final DataFrameColumn<R,C> add(C key, Class<?> type) {
        return addColumns().andThen(notifyEvent()).andThen(keys -> frame().colAt(key)).apply(columnMap -> {
            if (!contains(key)) {
                final int rowCapacity = frame().content().rowCapacity();
                columnMap.put(key, Array.of(type, rowCapacity));
            }
        });
    }


    @Override
    public <T> DataFrame<R,C> add(C key, Class<T> type, Function<DataFrameValue<R,C>,T> seeder) {
        return addColumns().andThen(seed(seeder)).andThen(notifyEvent()).andThen(x -> frame()).apply(columnMap -> {
            if (!contains(key)) {
                final int rowCapacity = frame().content().rowCapacity();
                columnMap.put(key, Array.of(type, rowCapacity));
            }
        });
    }


    @Override
    public final Array<C> addAll(Iterable<C> colKeys, Class<?> type) {
        return addColumns().andThen(notifyEvent()).apply(columnMap -> colKeys.forEach(colKey -> {
            if (!contains(colKey)) {
                columnMap.put(colKey, Array.of(type, frame().rowCount()));
            }
        }));
    }


    @Override
    public final Array<C> addAll(Consumer<Map<C,Iterable<?>>> consumer) {
        return addColumns().andThen(notifyEvent()).apply(consumer);
    }


    @Override
    public final Array<C> addAll(DataFrame<R,C> other) {
        final XDataFrame<R,C> target = frame();
        final XDataFrame<R,C> source = (XDataFrame<R,C>)other;
        final ArrayBuilder<C> builder = ArrayBuilder.of(source.colCount(), source.cols().keyType());
        final boolean ignoreDuplicates = DataFrameOptions.isIgnoreDuplicates();
        source.cols().forEach(column -> {
            final C colKey = column.key();
            final boolean exists = target.cols().contains(colKey);
            if (exists && !ignoreDuplicates) {
                throw new DataFrameException("A column for key already exists in this frame: " + colKey);
            } else if (!exists) {
                final Class<?> type = source.content().colType(colKey);
                final Array<?> colData = Array.of(type, target.rowCount());
                target.cols().add(colKey, colData);
                builder.add(colKey);
            }
        });
        final Array<C> colKeys = builder.toArray();
        if (colKeys.length() > 0) {
            final Array<R> rowKeys = source.rowKeys().intersect(target.rowKeys());
            XDataFrameCopy.apply(source, target, rowKeys, colKeys);
            notifyEvent().apply(colKeys);
        }
        return colKeys;
    }


    @Override
    public final DataFrameAxisStats<C,R,C,C,StatType> stats() {
        return new XDataFrameAxisStats<>(frame(), isParallel(), true);
    }


    @Override
    @SafeVarargs
    public final DataFrame<Double,C> hist(int binCount, C... columns) {
        return hist(binCount, (columns == null || columns.length == 0) ? keyArray() : Array.of(columns));
    }


    @Override
    public final DataFrame<Double,C> hist(int binCount, Iterable<C> columns) {
        Asserts.check(binCount > 0, "The bin count must be > 0");
        final DataFrame<R,C> filter = frame().cols().select(columns);
        final double minValue = filter.stats().min();
        final double maxValue = filter.stats().max();
        final double stepSize = (maxValue - minValue) / binCount;
        final Range<Double> rowKeys = Range.of(minValue, maxValue + stepSize, stepSize);
        final DataFrame<Double,C> hist = DataFrame.ofInts(rowKeys, columns);
        final XDataFrameColumn<R,C> colCursor = new XDataFrameColumn<>(frame(), false);
        columns.forEach(colKey -> {
            colCursor.moveTo(colKey);
            final int colOrdinal = hist.cols().ordinalOf(colKey);
            colCursor.forEachValue(v -> {
                final double value = v.getDouble();
                hist.rows().lowerKey(value).ifPresent(lowerKey -> {
                    final int rowOrdinal = hist.rows().ordinalOf(lowerKey);
                    final int count = hist.data().getInt(rowOrdinal, colOrdinal);
                    hist.data().setInt(rowOrdinal, colOrdinal, count + 1);
                });
            });
        });
        return hist;
    }


    @Override @Parallel
    public final DataFrame<R,C> sort(boolean ascending) {
        return XDataFrameSorter.sortCols(frame(), ascending, isParallel());
    }


    @Override @Parallel
    public final DataFrame<R,C> sort(boolean ascending, R key) {
        return XDataFrameSorter.sortCols(frame(), key, ascending, isParallel());
    }


    @Override @Parallel
    public final DataFrame<R,C> sort(boolean ascending, List<R> keys) {
        return XDataFrameSorter.sortCols(frame(), keys, ascending, isParallel());
    }


    @Override @Parallel
    public final DataFrame<R,C> sort(Comparator<DataFrameColumn<R,C>> comparator) {
        return XDataFrameSorter.sortCols(frame(), isParallel(), comparator);
    }

    @Override @Parallel
    public final DataFrame<R,C> apply(Consumer<DataFrameColumn<R,C>> consumer) {
        this.forEach(consumer);
        return frame();
    }

    @Override @Parallel
    public final DataFrame<R,C> demean(boolean inPlace) {
        if (!inPlace) {
            return frame().copy().cols().demean(true);
        } else {
            frame().cols().forEach(column -> {
                final ArrayType type = ArrayType.of(column.typeInfo());
                if (type.isInteger()) {
                    final int mean = column.stats().mean().intValue();
                    column.applyInts(v -> v.getInt() - mean);
                } else if (type.isLong()) {
                    final long mean = column.stats().mean().longValue();
                    column.applyLongs(v -> v.getLong() - mean);
                } else {
                    final double mean = column.stats().mean();
                    column.applyDoubles(v -> v.getDouble() - mean);
                }
            });
            return frame();
        }
    }


    @Override
    public final <X> DataFrame<R,X> mapKeys(Function<DataFrameColumn<R,C>,X> mapper) {
        if (frame().colKeys().isFilter()) {
            throw new DataFrameException("Column axis is immutable for this frame, call copy() first");
        } else {
            final XDataFrameColumn<R,C> column = new XDataFrameColumn<>(frame(), false);
            return frame().mapColKeys((key, ordinal) -> mapper.apply(column.moveTo(ordinal)));
        }
    }


    /**
     * Returns a function to seed values for a set of columns defined by the input keys
     * @param seeder        the function that will seed data for columns
     * @return              the seeder function
     */
    private <T> Function<Array<C>,Array<C>> seed(Function<DataFrameValue<R,C>,T> seeder) {
        final XDataFrameColumn<R,C> column = new XDataFrameColumn<>(frame(), false);
        return keys -> {
            keys.forEach(key -> {
                column.moveTo(key);
                column.applyValues(seeder);
            });
            return keys;
        };
    }

    /**
     * Returns a function that will publish notification events when called
     * @return      the function to send notification events
     */
    private Function<Array<C>,Array<C>> notifyEvent() {
        return keys -> {
            if (keys.length() > 0 && frame().events().isEnabled()) {
                final XDataFrame<R,C> frame = frame();
                final DataFrameEvent<R,C> event = DataFrameEvent.createColumnAdd(frame, keys);
                frame.events().fireDataFrameEvent(event);
            }
            return keys;
        };
    }

    /**
     * Returns a function that will add columns to the frame
     * @return  the function to add columns to the frame
     */
    private Function<Consumer<Map<C,Iterable<?>>>,Array<C>> addColumns() {
        return consumer -> {
            final Class<C> keyType = keyType();
            final Map<C,Iterable<?>> columnMap = new LinkedHashMap<>();
            consumer.accept(columnMap);
            final ArrayBuilder<C> builder = ArrayBuilder.of(columnMap.size(), keyType);
            columnMap.keySet().forEach(colKey -> {
                final Iterable<?> values = columnMap.get(colKey);
                final XDataFrameContent<R,C> content = frame().content();
                final boolean added = content.addColumn(colKey, values);
                if (added) {
                    builder.add(colKey);
                }
            });
            return builder.toArray();
        };
    }


    @Override
    public final DataFrame<C,StatType> describe(StatType... stats) {
        final Array<StatType> statKeys = Array.of(stats);
        final Array<C> colKeys = filter(DataFrameColumn::isNumeric).keyArray();
        final DataFrame<C,StatType> result = DataFrame.ofDoubles(colKeys, statKeys);
        this.filter(DataFrameColumn::isNumeric).forEach(column -> {
            final C key = column.key();
            final Stats<Double> colStats = column.stats();
            for (int j = 0; j < statKeys.length(); ++j) {
                final StatType stat = statKeys.getValue(j);
                final double value = stat.apply(colStats);
                result.data().setDouble(key, j, value);
            }
        });
        return result;
    }

}
