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

import com.zavtech.morpheus.array.ArrayType;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameCursor;
import com.zavtech.morpheus.frame.DataFrameRow;
import com.zavtech.morpheus.frame.DataFrameValue;
import com.zavtech.morpheus.index.Index;

/**
 * An implementation of DataFrameVector which represents a view onto a single row of an underlying DataFrame.
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrameRow<R,C> extends XDataFrameVector<R,C,R,C,DataFrameRow<R,C>> implements DataFrameRow<R,C> {

    private static final long serialVersionUID = 1L;

    private XDataFrame<R,C> frame;
    private XDataFrameContent<R,C>.Row row;

    /**
     * Constructor
     * @param frame     the frame reference
     * @param parallel  true for parallel implementation
     */
    XDataFrameRow(XDataFrame<R,C> frame, boolean parallel) {
        this(frame, parallel, -1);
    }

    /**
     * Constructor
     * @param frame         the frame reference
     * @param parallel      true for parallel implementation
     * @param rowOrdinal    the row ordinal in view space
     */
    XDataFrameRow(XDataFrame<R,C> frame, boolean parallel, int rowOrdinal) {
        super(frame, true, parallel);
        this.frame = frame;
        this.row = frame.content().rowCursorDirect();
        if (rowOrdinal >= 0) {
            this.moveTo(rowOrdinal);
        }
    }


    @Override()
    public final DataFrameRow<R,C> forEachValue(Consumer<DataFrameValue<R,C>> consumer) {
        final int colCount = frame.colCount();
        if (colCount > 0) {
            final int rowOrdinal = ordinal();
            final DataFrameCursor<R,C> cursor = frame.cursor().moveTo(rowOrdinal, 0);
            for (int i=0; i<colCount; ++i) {
                cursor.moveToColumn(i);
                consumer.accept(cursor);
            }
        }
        return this;
    }


    @Override
    public final Iterator<DataFrameValue<R,C>> iterator() {
        final int rowOrdinal = ordinal();
        final DataFrameCursor<R,C> cursor = frame.cursor().moveToRow(rowOrdinal);
        return new Iterator<DataFrameValue<R,C>>() {
            private int ordinal = 0;
            @Override
            public final DataFrameValue<R,C> next() {
                return cursor.moveToColumn(ordinal++);
            }
            @Override
            public final boolean hasNext() {
                return ordinal < frame.colCount();
            }
        };
    }


    @Override
    public final Iterator<DataFrameValue<R,C>> iterator(Predicate<DataFrameValue<R,C>> predicate) {
        final int rowOrdinal = ordinal();
        final DataFrameCursor<R,C> cursor = frame.cursor().moveToRow(rowOrdinal);
        return new Iterator<DataFrameValue<R,C>>() {
            private int ordinal = 0;
            @Override
            public final DataFrameValue<R,C> next() {
                return cursor.moveToColumn(ordinal++);
            }
            @Override
            public final boolean hasNext() {
                while (ordinal < frame.colCount()) {
                    cursor.moveToColumn(ordinal);
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
        value.moveToRow(ordinal());
        for (int i=0; i<size(); ++i) {
            value.moveToColumn(i);
            if (predicate.test(value)) {
                return Optional.of(value);
            }
        }
        return Optional.empty();
    }


    @Override()
    public final Optional<DataFrameValue<R,C>> last(Predicate<DataFrameValue<R,C>> predicate) {
        final DataFrameCursor<R,C> value = frame().cursor();
        value.moveToRow(ordinal());
        for (int i=size()-1; i>=0; --i) {
            value.moveToColumn(i);
            if (predicate.test(value)) {
                return Optional.of(value);
            }
        }
        return Optional.empty();
    }


    @Override()
    public final Optional<DataFrameValue<R,C>> min(Predicate<DataFrameValue<R,C>> predicate) {
        if (frame.rowCount() == 0 || frame.colCount() == 0) {
            return Optional.empty();
        } else {
            return first(predicate).map(first -> {
                final int colStart = first.colOrdinal();
                final DataFrameCursor<R,C> result = frame.cursor().moveTo(first);
                final DataFrameCursor<R,C> value = frame.cursor().moveTo(first);
                for (int i=colStart+1; i<frame.colCount(); ++i) {
                    value.moveToColumn(i);
                    if (predicate.test(value) && value.compareTo(result) < 0) {
                        result.moveTo(value);
                    }
                }
                return result;
            });
        }
    }


    @Override()
    public Optional<DataFrameValue<R, C>> max(Predicate<DataFrameValue<R, C>> predicate) {
        if (frame.rowCount() == 0 || frame.colCount() == 0) {
            return Optional.empty();
        } else {
            return first(predicate).map(first -> {
                final int colStart = first.colOrdinal();
                final DataFrameCursor<R,C> result = frame.cursor().moveTo(first);
                final DataFrameCursor<R,C> value = frame.cursor().moveTo(first);
                for (int i=colStart+1; i<frame.colCount(); ++i) {
                    value.moveToColumn(i);
                    if (predicate.test(value) && value.compareTo(result) > 0) {
                        result.moveTo(value);
                    }
                }
                return result;
            });
        }
    }


    @Override
    public final Optional<DataFrameValue<R,C>> min(Comparator<DataFrameValue<R, C>> comparator) {
        if (frame.rowCount() == 0 || frame.colCount() == 0) {
            return Optional.empty();
        } else {
            final DataFrameCursor<R,C> result = frame.cursor().moveTo(key(), 0);
            final DataFrameCursor<R,C> value = frame.cursor().moveTo(key(), 0);
            for (int i=0; i<frame.colCount(); ++i) {
                value.moveToColumn(i);
                if (comparator.compare(value, result) < 0) {
                    result.moveTo(value);
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
            final DataFrameCursor<R,C> result = frame.cursor().moveTo(key(), 0);
            final DataFrameCursor<R,C> value = frame.cursor().moveTo(key(), 0);
            for (int i=0; i<frame.colCount(); ++i) {
                value.moveToColumn(i);
                if (comparator.compare(value, result) > 0) {
                    result.moveTo(value);
                }
            }
            return Optional.of(result);
        }
    }


    @Override()
    public final DataFrame<R,C> rank() {
        final double[] values = toDoubleStream().toArray();
        final double[] ranks = XDataFrameRank.rank(values);
        final Index<C> colKeys = Index.of(frame.cols().keyArray());
        return DataFrame.ofDoubles(key(), colKeys).applyDoubles(v -> ranks[v.rowOrdinal()]);
    }


    @Override()
    public final XDataFrameRow<R,C> moveTo(R key) {
        this.row.moveTo(key);
        return this;
    }


    @Override()
    public final XDataFrameRow<R,C> moveTo(int ordinal) {
        this.row.moveTo(ordinal);
        return this;
    }


    @Override()
    public final boolean isNumeric() {
        return ArrayType.of(typeInfo()).isNumeric();
    }


    @Override
    public final DataFrameRow<R,C> parallel() {
        return new XDataFrameRow<>(frame, true, ordinal());
    }


    @Override
    public final R key() {
        return row.key();
    }

    @Override
    public final int ordinal() {
        return row.ordinal();
    }

    @Override
    public final Class<?> typeInfo() {
        return row.typeInfo();
    }

    @Override
    public final boolean getBoolean(C key) {
        return row.getBoolean(key);
    }

    @Override
    public final boolean getBoolean(int index) {
        return row.getBoolean(index);
    }

    @Override
    public final int getInt(C key) {
        return row.getInt(key);
    }

    @Override
    public final int getInt(int index) {
        return row.getInt(index);
    }

    @Override
    public final long getLong(C key) {
        return row.getLong(key);
    }

    @Override
    public final long getLong(int index) {
        return row.getLong(index);
    }

    @Override
    public final double getDouble(C key) {
        return row.getDouble(key);
    }

    @Override
    public final double getDouble(int index) {
        return row.getDouble(index);
    }

    @Override
    public final <V> V getValue(C key) {
        return row.getValue(key);
    }

    @Override
    public final <V> V getValue(int index) {
        return row.getValue(index);
    }

    @Override
    public final boolean setBoolean(int index, boolean value) {
        return row.setBoolean(index, value);
    }

    @Override
    public final boolean setBoolean(C key, boolean value) {
        return row.setBoolean(key, value);
    }

    @Override
    public final int setInt(int index, int value) {
        return row.setInt(index, value);
    }

    @Override
    public final int setInt(C key, int value) {
        return row.setInt(key, value);
    }

    @Override
    public final long setLong(int index, long value) {
        return row.setLong(index, value);
    }

    @Override
    public final long setLong(C key, long value) {
        return row.setLong(key, value);
    }

    @Override
    public final double setDouble(int index, double value) {
        return row.setDouble(index, value);
    }

    @Override
    public final double setDouble(C key, double value) {
        return row.setDouble(key, value);
    }

    @Override
    public final <V> V setValue(int index, V value) {
        return row.setValue(index, value);
    }

    @Override
    public final <V> V setValue(C key, V value) {
        return row.setValue(key, value);
    }

}
