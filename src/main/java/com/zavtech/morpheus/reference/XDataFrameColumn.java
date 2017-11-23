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
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayType;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameColumn;
import com.zavtech.morpheus.frame.DataFrameCursor;
import com.zavtech.morpheus.frame.DataFrameValue;
import com.zavtech.morpheus.util.Bounds;

/**
 * An implementation of DataFrameVector used to represent a column vector in a DataFrame.
 *
 * @param <C>   the column key type
 * @param <R>   the row key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrameColumn<R,C> extends XDataFrameVector<C,R,R,C,DataFrameColumn<R,C>> implements DataFrameColumn<R,C> {

    private static final long serialVersionUID = 1L;

    private XDataFrame<R,C> frame;
    private XDataFrameContent<R,C>.Column column;

    /**
     * Constructor
     * @param frame     the frame reference
     * @param parallel  true for parallel implementation
     */
    XDataFrameColumn(XDataFrame<R,C> frame, boolean parallel) {
        this(frame, parallel, -1);
    }

    /**
     * Constructor
     * @param frame     the frame reference
     * @param parallel  true for parallel implementation
     * @param ordinal   the column ordinal in view space
     */
    XDataFrameColumn(XDataFrame<R,C> frame, boolean parallel, int ordinal) {
        super(frame, false, parallel);
        this.frame = frame;
        this.column = frame.content().colCursorDirect();
        if (ordinal >= 0) {
            this.moveTo(ordinal);
        }
    }


    @Override()
    public final DataFrameColumn<R,C> forEachValue(Consumer<DataFrameValue<R,C>> consumer) {
        final int rowCount = frame.rowCount();
        if (rowCount > 0) {
            final int colOrdinal = ordinal();
            final DataFrameCursor<R,C> cursor = frame.cursor().moveTo(0, colOrdinal);
            for (int i=0; i<rowCount; ++i) {
                cursor.moveToRow(i);
                consumer.accept(cursor);
            }
        }
        return this;
    }


    @Override
    public final Iterator<DataFrameValue<R,C>> iterator() {
        final int colOrdinal = ordinal();
        final DataFrameCursor<R,C> cursor = frame.cursor().moveToColumn(colOrdinal);
        return new Iterator<DataFrameValue<R,C>>() {
            private int ordinal = 0;
            @Override
            public DataFrameValue<R,C> next() {
                return cursor.moveToRow(ordinal++);
            }
            @Override
            public boolean hasNext() {
                return ordinal < frame.rowCount();
            }
        };
    }


    @Override
    public final Iterator<DataFrameValue<R,C>> iterator(Predicate<DataFrameValue<R,C>> predicate) {
        final int colOrdinal = ordinal();
        final DataFrameCursor<R,C> cursor = frame.cursor().moveToColumn(colOrdinal);
        return new Iterator<DataFrameValue<R,C>>() {
            private int ordinal = 0;
            @Override
            public DataFrameValue<R,C> next() {
                return cursor.moveToRow(ordinal++);
            }
            @Override
            public boolean hasNext() {
                while (ordinal < frame.rowCount()) {
                    cursor.moveToRow(ordinal);
                    if (predicate == null || predicate.test(cursor)) {
                        return true;
                    } else {
                        ordinal++;
                    }
                }
                return false;
            }
        };
    }


    @Override()
    public final Optional<DataFrameValue<R,C>> first(Predicate<DataFrameValue<R,C>> predicate) {
        final DataFrameCursor<R,C> value = frame().cursor();
        value.moveToColumn(ordinal());
        for (int i=0; i<size(); ++i) {
            value.moveToRow(i);
            if (predicate.test(value)) {
                return Optional.of(value);
            }
        }
        return Optional.empty();
    }


    @Override()
    public final Optional<DataFrameValue<R,C>> last(Predicate<DataFrameValue<R,C>> predicate) {
        final DataFrameCursor<R,C> value = frame().cursor();
        value.moveToColumn(ordinal());
        for (int i=size()-1; i>=0; --i) {
            value.moveToRow(i);
            if (predicate.test(value)) {
                return Optional.of(value);
            }
        }
        return Optional.empty();
    }


    @Override()
    public final <V> Optional<V> min(Predicate<DataFrameValue<R,C>> predicate) {
        if (frame.rowCount() == 0 || frame.colCount() == 0) {
            return Optional.empty();
        } else {
            return first(predicate).map(first -> {
                final int rowStart = first.rowOrdinal();
                final DataFrameCursor<R,C> result = frame.cursor().moveTo(first.rowOrdinal(), first.colOrdinal());
                final DataFrameCursor<R,C> value = frame.cursor().moveTo(first.rowOrdinal(), first.colOrdinal());
                for (int i=rowStart+1; i<frame.rowCount(); ++i) {
                    value.moveToRow(i);
                    if (predicate.test(value) && value.compareTo(result) < 0) {
                        result.moveToRow(value.rowOrdinal());
                    }
                }
                return result;
            }).map(DataFrameValue::<V>getValue);
        }
    }


    @Override()
    public final <V> Optional<V> max(Predicate<DataFrameValue<R,C>> predicate) {
        if (frame.rowCount() == 0 || frame.colCount() == 0) {
            return Optional.empty();
        } else {
            return first(predicate).map(first -> {
                final int rowStart = first.rowOrdinal();
                final DataFrameCursor<R,C> result = frame.cursor().moveTo(first.rowOrdinal(), first.colOrdinal());
                final DataFrameCursor<R,C> value = frame.cursor().moveTo(first.rowOrdinal(), first.colOrdinal());
                for (int i=rowStart+1; i<frame.rowCount(); ++i) {
                    value.moveToRow(i);
                    if (predicate.test(value) && value.compareTo(result) > 0) {
                        result.moveToRow(value.rowOrdinal());
                    }
                }
                return result;
            }).map(DataFrameValue::<V>getValue);
        }
    }


    @Override
    public final Optional<DataFrameValue<R,C>> min(Comparator<DataFrameValue<R, C>> comparator) {
        if (frame.rowCount() == 0 || frame.colCount() == 0) {
            return Optional.empty();
        } else {
            final DataFrameCursor<R,C> result = frame.cursor().moveTo(0, key());
            final DataFrameCursor<R,C> value = frame.cursor().moveTo(0, key());
            for (int i=0; i<frame.rowCount(); ++i) {
                value.moveToRow(i);
                if (comparator.compare(value, result) < 0) {
                    result.moveToRow(value.rowOrdinal());
                }
            }
            return Optional.of(result);
        }
    }


    @Override
    public final Optional<DataFrameValue<R,C>> max(Comparator<DataFrameValue<R, C>> comparator) {
        if (frame.rowCount() == 0 || frame.colCount() == 0) {
            return Optional.empty();
        } else {
            final DataFrameCursor<R,C> result = frame.cursor().moveTo(0, key());
            final DataFrameCursor<R,C> value = frame.cursor().moveTo(0, key());
            for (int i=0; i<frame.rowCount(); ++i) {
                value.moveToRow(i);
                if (comparator.compare(value, result) > 0) {
                    result.moveToRow(value.rowOrdinal());
                }
            }
            return Optional.of(result);
        }
    }


    @Override
    public <V> Optional<Bounds<V>> bounds(Predicate<DataFrameValue<R,C>> predicate) {
        if (frame.rowCount() == 0 || frame.colCount() == 0) {
            return Optional.empty();
        } else {
            return first(predicate).map(first -> {
                final int rowStart = first.rowOrdinal();
                final DataFrameCursor<R,C> cursor = frame.cursor().moveTo(first.rowOrdinal(), first.colOrdinal());
                final DataFrameCursor<R,C> minValue = frame.cursor().moveTo(first.rowOrdinal(), first.colOrdinal());
                final DataFrameCursor<R,C> maxValue = frame.cursor().moveTo(first.rowOrdinal(), first.colOrdinal());
                for (int i=rowStart+1; i<frame.rowCount(); ++i) {
                    cursor.moveToRow(i);
                    if (predicate.test(cursor)) {
                        if (minValue.compareTo(cursor) < 0) {
                            minValue.moveToRow(cursor.rowOrdinal());
                        }
                        if (maxValue.compareTo(cursor) > 0) {
                            maxValue.moveToRow(cursor.rowOrdinal());
                        }
                    }
                }
                final V lower = minValue.getValue();
                final V upper = maxValue.getValue();
                return Bounds.of(lower, upper);
            });
        }
    }


    @Override()
    public final DataFrame<R,C> rank() {
        final double[] values = toDoubleStream().toArray();
        final double[] ranks = XDataFrameRank.rank(values);
        final Array<R> rowKeys = frame.rowKeys().toArray();
        return DataFrame.ofDoubles(rowKeys, key()).applyDoubles(v -> ranks[v.rowOrdinal()]);
    }


    @Override()
    public final boolean isNumeric() {
        return ArrayType.of(typeInfo()).isNumeric();
    }

    @Override
    public final DataFrameColumn<R, C> parallel() {
        return new XDataFrameColumn<>(frame, true, ordinal());
    }

    @Override
    public final XDataFrameColumn<R,C> moveTo(C key) {
        this.column.moveTo(key);
        return this;
    }

    @Override
    public final XDataFrameColumn<R,C> moveTo(int ordinal) {
        this.column.moveTo(ordinal);
        return this;
    }

    @Override
    public final C key() {
        return column.key();
    }

    @Override
    public final int ordinal() {
        return column.ordinal();
    }

    @Override
    public final Class<?> typeInfo() {
        return column.typeInfo();
    }

    @Override
    public final boolean getBoolean(R key) {
        return column.getBoolean(key);
    }

    @Override
    public final boolean getBoolean(int index) {
        return column.getBoolean(index);
    }

    @Override
    public final int getInt(R key) {
        return column.getInt(key);
    }

    @Override
    public final int getInt(int index) {
        return column.getInt(index);
    }

    @Override
    public final long getLong(R key) {
        return column.getLong(key);
    }

    @Override
    public final long getLong(int index) {
        return column.getLong(index);
    }

    @Override
    public final double getDouble(R key) {
        return column.getDouble(key);
    }

    @Override
    public final double getDouble(int index) {
        return column.getDouble(index);
    }

    @Override
    public final <V> V getValue(R key) {
        return column.getValue(key);
    }

    @Override
    public final <V> V getValue(int index) {
        return column.getValue(index);
    }

    @Override
    public final boolean setBoolean(int index, boolean value) {
        return column.setBoolean(index, value);
    }

    @Override
    public final boolean setBoolean(R key, boolean value) {
        return column.setBoolean(key, value);
    }

    @Override
    public final int setInt(int index, int value) {
        return column.setInt(index, value);
    }

    @Override
    public final int setInt(R key, int value) {
        return column.setInt(key, value);
    }

    @Override
    public final long setLong(int index, long value) {
        return column.setLong(index, value);
    }

    @Override
    public final long setLong(R key, long value) {
        return column.setLong(key, value);
    }

    @Override
    public final double setDouble(int index, double value) {
        return column.setDouble(index, value);
    }

    @Override
    public final double setDouble(R key, double value) {
        return column.setDouble(key, value);
    }

    @Override
    public final <V> V setValue(int index, V value) {
        return column.setValue(index, value);
    }

    @Override
    public final <V> V setValue(R key, V value) {
        return column.setValue(key, value);
    }

}
