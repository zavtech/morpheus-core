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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayType;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameColumns;
import com.zavtech.morpheus.frame.DataFrameCursor;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameFactory;
import com.zavtech.morpheus.frame.DataFrameHeader;
import com.zavtech.morpheus.frame.DataFrameRead;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.range.Range;

/**
 * The reference implementation of the DataFrameFactory interface.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class XDataFrameFactory extends DataFrameFactory {

    private DataFrameRead read = new XDataFrameRead();

    @Override
    public DataFrameRead read() {
        return read;
    }


    @Override
    @SuppressWarnings("unchecked")
    public <R,C> DataFrame<R,C> empty() {
        return new XDataFrame<>(Index.of((Class<R>)Object.class, 10000), Index.of((Class<C>)Object.class, 10), Object.class, false);
    }


    @Override
    public <R,C> DataFrame<R,C> empty(Class<R> rowAxisType, Class<C> colAxisType) {
        return new XDataFrame<>(Index.of(rowAxisType, 10000), Index.of(colAxisType, 10), Object.class, false);
    }


    @Override
    @SuppressWarnings("unchecked")
    public <R,C> DataFrame<R,C> combineFirst(Iterator<DataFrame<R,C>> iterator) {
        if (!iterator.hasNext()) {
            return DataFrame.empty();
        } else {
            final DataFrame<R,C> result = iterator.next().copy();
            final DataFrameCursor<R,C> cursor = result.cursor();
            while (iterator.hasNext()) {
                final DataFrame<R,C> next = iterator.next();
                result.rows().addAll(next);
                result.cols().addAll(next);
                next.cols().forEach(column -> {
                    final ArrayType type = ArrayType.of(column.typeInfo());
                    column.forEach(v -> {
                        final R rowKey = v.rowKey();
                        final C colKey = v.colKey();
                        if (cursor.atKeys(rowKey, colKey).isNull()) {
                            switch (type) {
                                case BOOLEAN:   cursor.setBoolean(v.getBoolean());  break;
                                case INTEGER:   cursor.setInt(v.getInt());          break;
                                case LONG:      cursor.setLong(v.getLong());        break;
                                case DOUBLE:    cursor.setDouble(v.getDouble());    break;
                                default:        cursor.setValue(v.getValue());      break;
                            }
                        }
                    });
                });
            }
            return result.rows().sort(true);
        }
    }


    @Override
    public <R,C> DataFrame<R,C> concatRows(Iterator<DataFrame<R,C>> iterator) {
        if (!iterator.hasNext()) {
            return DataFrame.empty();
        } else {
            final DataFrame<R,C> result = iterator.next().copy();
            while (iterator.hasNext()) {
                final DataFrame<R,C> next = iterator.next();
                if (next != null) {
                    result.rows().addAll(next);
                }
            }
            return result;
        }
    }


    @Override
    public <R,C> DataFrame<R,C> concatColumns(Iterator<DataFrame<R,C>> iterator) {
        if (!iterator.hasNext()) {
            return DataFrame.empty();
        } else {
            final DataFrame<R,C> result = iterator.next().copy();
            while (iterator.hasNext()) {
                final DataFrame<R,C> next = iterator.next();
                if (next != null) {
                    result.cols().addAll(next);
                }
            }
            return result;
        }
    }


    @SuppressWarnings("unchecked")
    public <R,C> DataFrame<R,C> from(Class<R> rowType, Map<C,Class<?>> columnMap) {
        final Set<Class> colKeySet = columnMap.keySet().stream().map(C::getClass).collect(Collectors.toSet());
        final Class<C> colType = colKeySet.size() == 1  ? colKeySet.iterator().next() : (Class<C>)Object.class;
        return from(rowType, colType, columns -> columnMap.forEach(columns::add));
    }


    @SuppressWarnings("unchecked")
    public <R,C> DataFrame<R,C> from(Iterable<R> rowKeys, Map<C,Class<?>> columnMap) {
        final Set<Class> colKeySet = columnMap.keySet().stream().map(C::getClass).collect(Collectors.toSet());
        final Class<C> colType = colKeySet.size() == 1  ? colKeySet.iterator().next() : (Class<C>)Object.class;
        return from(rowKeys, colType, columns -> columnMap.forEach(columns::add));
    }


    @Override
    public <R,C> DataFrame<R,C> from(Class<R> rowType, Class<C> colType) {
        return new XDataFrame<>(Index.of(rowType, 1000), Index.of(colType, 50), Object.class, false);
    }


    @Override
    public <R,C> DataFrame<R,C> from(Iterable<R> rowKeys, Iterable<C> colKeys, Class<?> type) {
        return new XDataFrame<>(toIndex(rowKeys), toIndex(colKeys), type, false);
    }


    @Override
    public <R,C> DataFrame<R,C> from(Iterable<R> rowKeys, Class<C> colType, Consumer<DataFrameColumns<R, C>> columns) {
        return new XDataFrame<>(toIndex(rowKeys), Index.of(colType, 50), Object.class, false).configure(columns);
    }


    @Override
    public <R,C> DataFrame<R,C> from(Class<R> rowType, Class<C> colType, Consumer<DataFrameColumns<R,C>> columns) {
        return new XDataFrame<>(Index.of(rowType, 1000), Index.of(colType, 50), Object.class, false).configure(columns);
    }


    @Override
    @SuppressWarnings("unchecked")
    public <R> DataFrame<R,String> from(ResultSet resultSet, int rowCapacity, Function<ResultSet, R> rowKeyFunction) throws SQLException {
        try {
            if (!resultSet.next()) {
                return createFrame(Index.empty(), resultSet);
            } else {
                final R rowKey = rowKeyFunction.apply(resultSet);
                final Index<R> rowIndex = Index.of((Class<R>)rowKey.getClass(), rowCapacity);
                final DataFrame<R,String> frame = createFrame(rowIndex, resultSet);
                final ResultSetMetaData metaData = resultSet.getMetaData();
                final int columnCount = metaData.getColumnCount();
                while (resultSet.next()) {
                    final R key = rowKeyFunction.apply(resultSet);
                    frame.rows().add(key);
                    final int rowOrdinal = frame.rowCount()-1;
                    for (int i=1; i<=columnCount; ++i) {
                        final int colOrdinal = i-1;
                        switch (metaData.getColumnType(i)) {
                            case Types.BIT:         frame.data().setBoolean(rowOrdinal, colOrdinal, resultSet.getBoolean(i));  break;
                            case Types.NVARCHAR:    frame.data().setValue(rowOrdinal, colOrdinal, resultSet.getString(i));     break;
                            case Types.CLOB:        frame.data().setValue(rowOrdinal, colOrdinal, resultSet.getString(i));     break;
                            case Types.VARCHAR:     frame.data().setValue(rowOrdinal, colOrdinal, resultSet.getString(i));     break;
                            case Types.BOOLEAN:     frame.data().setBoolean(rowOrdinal, colOrdinal, resultSet.getBoolean(i));  break;
                            case Types.DECIMAL:     frame.data().setDouble(rowOrdinal, colOrdinal, resultSet.getDouble(i));    break;
                            case Types.DATE:        frame.data().setValue(rowOrdinal, colOrdinal, resultSet.getDate(i));       break;
                            case Types.FLOAT:       frame.data().setDouble(rowOrdinal, colOrdinal, resultSet.getFloat(i));     break;
                            case Types.INTEGER:     frame.data().setInt(rowOrdinal, colOrdinal, resultSet.getInt(i));          break;
                            case Types.TINYINT:     frame.data().setInt(rowOrdinal, colOrdinal, resultSet.getShort(i));        break;
                            case Types.SMALLINT:    frame.data().setInt(rowOrdinal, colOrdinal, resultSet.getInt(i));          break;
                            case Types.BIGINT:      frame.data().setLong(rowOrdinal, colOrdinal, resultSet.getLong(i));        break;
                            case Types.TIMESTAMP:   frame.data().setValue(rowOrdinal, colOrdinal, resultSet.getTimestamp(i));  break;
                            case Types.DOUBLE:      frame.data().setDouble(rowOrdinal, colOrdinal, resultSet.getDouble(i));    break;
                            case Types.NUMERIC:     frame.data().setDouble(rowOrdinal, colOrdinal, resultSet.getDouble(i));    break;
                            case Types.CHAR:        frame.data().setValue(rowOrdinal, colOrdinal, resultSet.getString(i));     break;
                            default:                frame.data().setValue(rowOrdinal, colOrdinal, resultSet.getObject(i));     break;
                        }
                    }
                }
                return frame;
            }
        } catch (Throwable t) {
            throw new DataFrameException("Failed to initialize DataFrame from ResultSet: " + t.getMessage(), t);
        }
    }


    /**
     * Returns a newly created empty DataFrame from the ResultSet
     * @param rowIndex      the index of row keys
     * @param resultSet     the result set
     * @return              the newly created empty DataFrame
     * @throws SQLException if fails to parse result set
     */
    private <R> DataFrame<R,String> createFrame(Index<R> rowIndex, ResultSet resultSet) throws SQLException {
        return DataFrame.of(rowIndex, String.class, columns -> {
            try {
                final ResultSetMetaData metaData = resultSet.getMetaData();
                final int columnCount = metaData.getColumnCount();
                final DataFrameHeader.Builder<String> builder = new DataFrameHeader.Builder<>();
                for (int i=1; i<=columnCount; ++i) {
                    final String name = metaData.getColumnName(i);
                    switch (metaData.getColumnType(i)) {
                        case Types.BIT:         builder.add(name, Boolean.class);       break;
                        case Types.NVARCHAR:    builder.add(name, String.class);        break;
                        case Types.CLOB:        builder.add(name, String.class);        break;
                        case Types.VARCHAR:     builder.add(name, String.class);        break;
                        case Types.BOOLEAN:     builder.add(name, Boolean.class);       break;
                        case Types.DECIMAL:     builder.add(name, Double.class);        break;
                        case Types.DATE:        builder.add(name, Date.class);          break;
                        case Types.FLOAT:       builder.add(name, Double.class);        break;
                        case Types.INTEGER:     builder.add(name, Integer.class);       break;
                        case Types.TINYINT:     builder.add(name, Integer.class);       break;
                        case Types.SMALLINT:    builder.add(name, Integer.class);       break;
                        case Types.BIGINT:      builder.add(name, Long.class);          break;
                        case Types.TIMESTAMP:   builder.add(name, LocalDateTime.class); break;
                        case Types.DOUBLE:      builder.add(name, Double.class);        break;
                        case Types.NUMERIC:     builder.add(name, Double.class);        break;
                        case Types.CHAR:        builder.add(name, String.class);        break;
                        default:                builder.add(name, Object.class);        break;
                    }
                }
            } catch (SQLException ex) {
                throw new DataFrameException("Failed to create DataFrame from ResultSet", ex);
            }
        });
    }


    /**
     * Returns an Index created from the iterable of keys
     * @param keys  the keys to create Index from
     * @param <T>   the element type for Iterable
     * @return      the Index from Iterable
     */
    @SuppressWarnings("unchecked")
    private <T> Index<T> toIndex(Iterable<T> keys) {
        Index<T> index;
        if (keys instanceof Index) {
            index = (Index<T>)keys;
        } else if (keys instanceof Array) {
            final Array<T> array = (Array<T>)keys;
            index = Index.of(array);
        } else if (keys instanceof Range) {
            final Range<T> range = (Range<T>)keys;
            final Array<T> array = range.toArray();
            index = Index.of(array);
        } else if (keys instanceof Collection) {
            final Collection<T> collection = (Collection<T>)keys;
            index = Index.of(collection);
        } else {
            final List<T> list = new ArrayList<>();
            for (T key: keys) list.add(key);
            index = Index.of(list);
        }
        return index;
    }
}
