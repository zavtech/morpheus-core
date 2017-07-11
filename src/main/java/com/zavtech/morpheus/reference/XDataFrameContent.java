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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBuilder;
import com.zavtech.morpheus.array.ArrayType;
import com.zavtech.morpheus.array.ArrayUtils;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameColumn;
import com.zavtech.morpheus.frame.DataFrameContent;
import com.zavtech.morpheus.frame.DataFrameCursor;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameOptions;
import com.zavtech.morpheus.frame.DataFrameRow;
import com.zavtech.morpheus.frame.DataFrameValue;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.index.IndexException;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.util.Mapper;
import com.zavtech.morpheus.util.functions.ToBooleanFunction;

/**
 * A class that encapsulates the contents of a DataFrame, including row axis, column axis and data.
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrameContent<R,C> implements DataFrameContent<R,C>, Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    private Index<R> rowKeys;
    private Index<C> colKeys;
    private boolean columnStore;
    private List<Array<?>> data;


    /**
     * Constructor
     * @param rowKeys  the row key index
     * @param colKeys  the column key index
     */
    @SuppressWarnings("unchecked")
    XDataFrameContent(Iterable<R> rowKeys, Iterable<C> colKeys, Class<?> dataType) {
        this(rowKeys, colKeys, true, new ArrayList<>());
        this.data = new ArrayList<>(this.rowKeys.capacity());
        final int rowCapacity = rowDim().capacity();
        this.colKeys.keys().forEach(colKey -> {
            final Array<?> array = Array.of(dataType, rowCapacity);
            this.data.add(array);
        });
    }

    /**
     * Private constructor used to create filters on contents
     * @param rowKeys       the row axis
     * @param colKeys       the column axis
     * @param columnStore   true to store data in column major form
     * @param data          the data payload
     */
    private XDataFrameContent(Iterable<R> rowKeys, Iterable<C> colKeys, boolean columnStore, List<Array<?>> data) {
        this.columnStore = columnStore;
        this.rowKeys = toIndex(rowKeys);
        this.colKeys = toIndex(colKeys);
        this.data = data;
    }


    /**
     * Returns a newly created dimension for the Iterable provided
     * @param keys  the Iterable collection of keys
     * @param <K>   the key type
     * @return      the newly created dimension with keys
     */
    @SuppressWarnings("unchecked")
    private <K> Index<K> toIndex(Iterable<K> keys) {
        if (keys instanceof Index) {
            return (Index<K>)keys;
        } else if (keys instanceof Array) {
            return Index.of(keys);
        } else if (keys instanceof Range) {
            return Index.of(((Range<K>)keys).toArray());
        } else {
            final Class<K> keyType = (Class<K>)keys.iterator().next().getClass();
            final Array<K> array = ArrayBuilder.of(1000, keyType).addAll(keys).toArray();
            return Index.of(array);
        }
    }

    /**
     * Returns true if data is stored in columns, false if row store
     * @return  true if data is stored as columns
     */
    private boolean isColumnStore() {
        return columnStore;
    }

    /**
     * Returns the row capacity for this content
     * @return      the row capacity for content
     */
    final int rowCapacity() {
        if (!isColumnStore() || data.size() == 0) {
            return rowKeys.capacity();
        } else {
            return data.get(0).length();
        }
    }

    /**
     * Returns the row dimension for this content
     * @return      the row dimension for content
     */
    final Index<R> rowDim() {
        return rowKeys;
    }

    /**
     * Returns the column dimension for this content
     * @return  the column dimension for content
     */
    final Index<C> colDim() {
        return colKeys;
    }

    /**
     * Returns the transpose of this content
     * @return  the transpose of this content
     */
    final XDataFrameContent<C,R> transpose() {
        return new XDataFrameContent<>(colKeys, rowKeys, !isColumnStore(), data);
    }

    /**
     * Returns a newly created cursor for this content
     * @param frame the frame reference
     * @return  the newly created cursor
     */
    final DataFrameCursor<R,C> cursor(XDataFrame<R,C> frame) {
        return new Cursor(frame,  rowKeys.isEmpty() ? -1 : 0, colKeys.isEmpty() ? -1 : 0);
    }

    /**
     * Returns the most specific type that can be used to describe all elements
     * @return      the most specific type that can be used to describe all elements
     */
    private Class<?> typeInfo() {
        final Set<Class<?>> typeSet = new HashSet<>();
        this.data.forEach(array -> typeSet.add(array.type()));
        if (typeSet.size() == 1) {
            return typeSet.iterator().next();
        } else {
            return Object.class;
        }
    }

    /**
     * Returns a stream of types to describe each row in this content
     * @return  the stream of row types
     */
    final Stream<Class<?>> rowTypes() {
        return isColumnStore() ? IntStream.range(0, rowKeys.size()).mapToObj(i -> typeInfo()) : data.stream().map(Array::type);
    }

    /**
     * Returns a stream of types to describe each column in this content
     * @return  the stream of column types
     */
    final Stream<Class<?>> colTypes() {
        return isColumnStore() ? data.stream().map(Array::type) : IntStream.range(0, colKeys.size()).mapToObj(i -> typeInfo());
    }


    /**
     * Returns the array type for the vector implied by the row key specified
     * @param rowKey    the row key
     * @return          the array type for row key
     */
    final Class<?> rowType(R rowKey) {
        if (isColumnStore()) {
            return typeInfo();
        } else {
            final int rowIndex = rowKeys.getIndexForKey(rowKey);
            return data.get(rowIndex).type();
        }
    }


    /**
     * Returns the array type for the vector implied by the column key specified
     * @param colKey    the row key
     * @return          the array type for column key
     */
    final Class<?> colType(C colKey) {
        if (!isColumnStore()) {
            return typeInfo();
        } else {
            final int colIndex = colKeys.getIndexForKey(colKey);
            return data.get(colIndex).type();
        }
    }


    /**
     * Returns the array for the row or column key specified
     * @param key   the row or column key if this is row or column major respectively
     * @return      the array for the key specified
     * @throws IndexException if no match for the key
     */
    @SuppressWarnings("unchecked")
    private <T> Array<T> getArray(Object key) {
        if (isColumnStore()) {
            final int index = colKeys.getIndexForKey((C)key);
            return (Array<T>)data.get(index);
        } else {
            final int index = rowKeys.getIndexForKey((R)key);
            return (Array<T>)data.get(index);
        }
    }


    /**
     * Returns a copy of this content replacing the row keys with the arg
     * @param dimension  the row keys replacement
     * @param <T>   the new row key type
     * @return      shallow copy of this content
     */
    final <T> XDataFrameContent<T,C> withRowKeys(Index<T> dimension) {
        return new XDataFrameContent<>(dimension, colKeys, columnStore, data);
    }


    /**
     * Returns a copy of this content replacing the column keys with the arg
     * @param dimension  the column keys replacement
     * @param <T>   the new column key type
     * @return      shallow copy of this content
     */
    final <T> XDataFrameContent<R,T> withColKeys(Index<T> dimension) {
        return new XDataFrameContent<>(rowKeys, dimension, columnStore, data);
    }


    /**
     * Adds a new row to this content if it does not already exist
     * @param rowKey    the row key to add
     * @return          true if added
     */
    final boolean addRow(R rowKey) {
        if (!isColumnStore()) {
            throw new DataFrameException("Cannot add rows to a transposed DataFrame, call transpose() and then add columns");
        } else if (this.rowKeys.isFilter()) {
            throw new DataFrameException("Cannot add keys to a filtered axis of a DataFrame");
        } else {
            final boolean added = rowKeys.add(rowKey);
            final int rowCount = rowKeys.size();
            this.ensureCapacity(rowCount);
            return added;
        }
    }


    /**
     * Adds multiple new rows to this content based on the keys provided
     * @param rowKeys   the row keys to add
     * @return          the array of keys added
     * @throws DataFrameException   if keys already exist, and silent is false
     */
    final Array<R> addRows(Iterable<R> rowKeys) throws DataFrameException {
        if (!isColumnStore()) {
            throw new DataFrameException("This DataFrame is configured as a row store, transpose() first");
        } else if (this.rowKeys.isFilter()) {
            throw new DataFrameException("Cannot add keys to a sliced axis of a DataFrame");
        } else {
            final int preSize = this.rowKeys.size();
            final Class<R> type = this.rowKeys.type();
            final boolean ignoreDuplicates = DataFrameOptions.isIgnoreDuplicates();
            final int count = this.rowKeys.addAll(rowKeys, ignoreDuplicates);
            final Array<R> added = Array.of(type, count);
            for (int i=0; i<count; ++i) {
                final R key = this.rowKeys.getKey(preSize + i);
                added.setValue(i, key);
            }
            final int rowCount = this.rowKeys.size();
            this.ensureCapacity(rowCount);
            return added;
        }
    }


    /**
     * Ensures that the data arrays support the capacity of row index
     * @param rowCount      the row count for current row index
     */
    private void ensureCapacity(int rowCount) {
        if (data.size() > 0) {
            final int capacity = rowCapacity();
            if (rowCount > capacity) {
                final int newCapacity = capacity + (capacity >> 1);
                if (newCapacity < rowCount) {
                    System.out.println("Expanding-1 row capacity of arrays to " + rowCount);
                    this.data.forEach(s -> s.expand(rowCount));
                } else {
                    System.out.println("Expanding-2 row capacity of arrays to " + newCapacity);
                    this.data.forEach(s -> s.expand(newCapacity));
                }
            }
        }
    }


    /**
     * Adds a column with the key and array data provided
     * @param key       the column key
     * @param values    the values for column
     * @return          true if the column did not already exist
     */
    @SuppressWarnings("unchecked")
    final <T> boolean addColumn(C key, Iterable<T> values) {
        if (!isColumnStore()) {
            throw new DataFrameException("This DataFrame is configured as a row store, transpose() first");
        } else {
            final boolean added = colKeys.add(key);
            if (added) {
                final Array<T> array = ArrayUtils.toArray(values);
                final int rowCapacity = rowCapacity();
                array.expand(rowCapacity);
                this.data.add(array);
            }
            return added;
        }
    }


    /**
     * Maps the specified column to booleans using the mapper function provided
     * @param frame     the frame reference
     * @param colKey    the column key to apply mapper function to
     * @param mapper    the mapper function to apply
     * @return          the newly created content, with update column
     */
    @SuppressWarnings("unchecked")
    final XDataFrameContent<R,C> mapToBooleans(XDataFrame<R,C> frame, C colKey, ToBooleanFunction<DataFrameValue<R,C>> mapper) {
        if (!isColumnStore()) {
            throw new DataFrameException("Cannot apply columns of a transposed DataFrame");
        } else {
            final int rowCount = rowKeys.size();
            final boolean parallel  = frame.isParallel();
            final int colIndex = colKeys.getIndexForKey(colKey);
            return new XDataFrameContent<>(rowKeys, colKeys, true, Mapper.apply(data, parallel, (index, array) -> {
                if (index != colIndex) {
                    return array;
                } else {
                    final int colOrdinal = colKeys.getOrdinalForKey(colKey);
                    final Array<?> targetValues = Array.of(Boolean.class, array.length());
                    final Cursor cursor = new Cursor(frame, rowKeys.isEmpty() ? -1 : 0, colOrdinal);
                    for (int i = 0; i < rowCount; ++i) {
                        cursor.moveToRow(i);
                        final boolean value = mapper.applyAsBoolean(cursor);
                        targetValues.setBoolean(cursor.rowIndex, value);
                    }
                    return targetValues;
                }
            }));
        }
    }


    /**
     * Maps the specified column to ints using the mapper function provided
     * @param frame     the frame reference
     * @param colKey    the column key to apply mapper function to
     * @param mapper    the mapper function to apply
     * @return          the newly created content, with update column
     */
    @SuppressWarnings("unchecked")
    final XDataFrameContent<R,C> mapToInts(XDataFrame<R,C> frame, C colKey, ToIntFunction<DataFrameValue<R,C>> mapper) {
        if (!isColumnStore()) {
            throw new DataFrameException("Cannot apply columns of a transposed DataFrame");
        } else {
            final int rowCount = rowKeys.size();
            final boolean parallel  = frame.isParallel();
            final int colIndex = colKeys.getIndexForKey(colKey);
            return new XDataFrameContent<>(rowKeys, colKeys, true, Mapper.apply(data, parallel, (index, array) -> {
                if (index != colIndex) {
                    return array;
                } else {
                    final int colOrdinal = colKeys.getOrdinalForKey(colKey);
                    final Array<?> targetValues = Array.of(Integer.class, array.length());
                    final Cursor cursor = new Cursor(frame, rowKeys.isEmpty() ? -1 : 0, colOrdinal);
                    for (int i = 0; i < rowCount; ++i) {
                        cursor.moveToRow(i);
                        final int value = mapper.applyAsInt(cursor);
                        targetValues.setInt(cursor.rowIndex, value);
                    }
                    return targetValues;
                }
            }));
        }
    }


    /**
     * Maps the specified column to longs using the mapper function provided
     * @param frame     the frame reference
     * @param colKey    the column key to apply mapper function to
     * @param mapper    the mapper function to apply
     * @return          the newly created content, with update column
     */
    @SuppressWarnings("unchecked")
    final XDataFrameContent<R,C> mapToLongs(XDataFrame<R,C> frame, C colKey, ToLongFunction<DataFrameValue<R,C>> mapper) {
        if (!isColumnStore()) {
            throw new DataFrameException("Cannot apply columns of a transposed DataFrame");
        } else {
            final int rowCount = rowKeys.size();
            final boolean parallel  = frame.isParallel();
            final int colIndex = colKeys.getIndexForKey(colKey);
            return new XDataFrameContent<>(rowKeys, colKeys, true, Mapper.apply(data, parallel, (index, array) -> {
                if (index != colIndex) {
                    return array;
                } else {
                    final int colOrdinal = colKeys.getOrdinalForKey(colKey);
                    final Array<?> targetValues = Array.of(Long.class, array.length());
                    final Cursor cursor = new Cursor(frame, rowKeys.isEmpty() ? -1 : 0, colOrdinal);
                    for (int i = 0; i < rowCount; ++i) {
                        cursor.moveToRow(i);
                        final long value = mapper.applyAsLong(cursor);
                        targetValues.setLong(cursor.rowIndex, value);
                    }
                    return targetValues;
                }
            }));
        }
    }


    /**
     * Maps the specified column to doubles using the mapper function provided
     * @param frame     the frame reference
     * @param colKey    the column key to apply mapper function to
     * @param mapper    the mapper function to apply
     * @return          the newly created content, with update column
     */
    @SuppressWarnings("unchecked")
    final XDataFrameContent<R,C> mapToDoubles(XDataFrame<R,C> frame, C colKey, ToDoubleFunction<DataFrameValue<R,C>> mapper) {
        if (!isColumnStore()) {
            throw new DataFrameException("Cannot apply columns of a transposed DataFrame");
        } else {
            final int rowCount = rowKeys.size();
            final boolean parallel  = frame.isParallel();
            final int colIndex = colKeys.getIndexForKey(colKey);
            return new XDataFrameContent<>(rowKeys, colKeys, true, Mapper.apply(data, parallel, (index, array) -> {
                if (index != colIndex) {
                    return array;
                } else {
                    final int colOrdinal = colKeys.getOrdinalForKey(colKey);
                    final Array<?> targetValues = Array.of(Double.class, array.length());
                    final Cursor cursor = new Cursor(frame, rowKeys.isEmpty() ? -1 : 0, colOrdinal);
                    for (int i = 0; i < rowCount; ++i) {
                        cursor.moveToRow(i);
                        final double value = mapper.applyAsDouble(cursor);
                        targetValues.setDouble(cursor.rowIndex, value);
                    }
                    return targetValues;
                }
            }));
        }
    }


    /**
     * Maps the specified column to objects using the mapper function provided
     * @param frame     the frame reference
     * @param colKey    the column key to apply mapper function to
     * @param type      the data type for mapped column
     * @param mapper    the mapper function to apply
     * @return          the newly created content, with update column
     */
    @SuppressWarnings("unchecked")
    final <T> XDataFrameContent<R,C> mapToObjects(XDataFrame<R,C> frame, C colKey, Class<T> type, Function<DataFrameValue<R,C>,T> mapper) {
        if (!isColumnStore()) {
            throw new DataFrameException("Cannot apply columns of a transposed DataFrame");
        } else {
            final int rowCount = rowKeys.size();
            final boolean parallel  = frame.isParallel();
            final int colIndex = colKeys.getIndexForKey(colKey);
            return new XDataFrameContent<>(rowKeys, colKeys, true, Mapper.apply(data, parallel, (index, array) -> {
                if (index != colIndex) {
                    return array;
                } else {
                    final int colOrdinal = colKeys.getOrdinalForKey(colKey);
                    final Array<T> targetValues = Array.of(type, array.length());
                    final Cursor cursor = new Cursor(frame, rowKeys.isEmpty() ? -1 : 0, colOrdinal);
                    for (int i = 0; i < rowCount; ++i) {
                        cursor.moveToRow(i);
                        final T value = mapper.apply(cursor);
                        targetValues.setValue(cursor.rowIndex, value);
                    }
                    return targetValues;
                }
            }));
        }
    }


    /**
     * Returns a filter of this contents based on the row and column keys provided
     * @param newRowKeys   the optionally filtered row keys
     * @param newColKeys   the optionally filtered column keys
     */
    final XDataFrameContent<R,C> filter(Index<R> newRowKeys, Index<C> newColKeys) {
        if (newColKeys.size() == this.colKeys.size()) {
            return new XDataFrameContent<>(newRowKeys, newColKeys, columnStore, data);
        } else if (columnStore) {
            final IntStream indexes = newColKeys.keys().mapToInt(k -> this.colKeys.getIndexForKey(k));
            final Index<C> colAxis = Index.of(newColKeys.toArray());
            final List<Array<?>> newData = indexes.mapToObj(data::get).collect(Collectors.toList());
            return new XDataFrameContent<>(newRowKeys, colAxis, columnStore, newData);
        } else {
            final IntStream indexes = newRowKeys.keys().mapToInt(k -> this.rowKeys.getIndexForKey(k));
            final Index<C> colAxis = Index.of(newColKeys.toArray());
            final List<Array<?>> newData = indexes.mapToObj(data::get).collect(Collectors.toList());
            return new XDataFrameContent<>(newRowKeys, colAxis, columnStore, newData);
        }
    }


    /**
     * Returns a newly created comparator to sort this content in the row dimension
     * @param colKeys       the column keys to sort rows by, in order of precedence
     * @param multiplier    the multiplier to control ascending / descending
     * @return              the newly created comparator
     */
    final XDataFrameComparator createRowComparator(List<C> colKeys, int multiplier) {
        final XDataFrameComparator[] comparators = new XDataFrameComparator[colKeys.size()];
        for (int i=0; i<colKeys.size(); ++i) {
            final C colKey = colKeys.get(i);
            final Array<?> array = getColArray(colKey);
            comparators[i] = XDataFrameComparator.create(array, multiplier);
        }
        return XDataFrameComparator.create(comparators).withIndex(rowKeys);
    }


    /**
     * Returns a newly created comparator to sort this content in the column dimension
     * @param rowKeys       the row keys to sort columns by, in order of precedence
     * @param multiplier    the multiplier to control ascending / descending
     * @return              the newly created comparator
     */
    final XDataFrameComparator createColComparator(List<R> rowKeys, int multiplier) {
        final XDataFrameComparator[] comparators = new XDataFrameComparator[rowKeys.size()];
        for (int i=0; i<rowKeys.size(); ++i) {
            final R rowKey = rowKeys.get(i);
            final Array<?> array = getRowArray(rowKey);
            comparators[i] = XDataFrameComparator.create(array, multiplier);
        }
        return XDataFrameComparator.create(comparators).withIndex(colKeys);
    }


    /**
     * Returns row data as an array for internal use only
     * @param rowKey    the row key
     * @return          the array of row data
     */
    private Array<?> getRowArray(R rowKey) {
        if (!columnStore) {
            final int index = rowKeys.getIndexForKey(rowKey);
            return data.get(index);
        } else {
            final Class<?> type = rowType(rowKey);
            final Array<?> array = Array.of(type, colKeys.size());
            final Vector<R,C> vector = rowCursor().moveTo(rowKey);
            switch (ArrayType.of(type)) {
                case BOOLEAN:           return array.applyBooleans(v -> vector.getBoolean(v.index()));
                case INTEGER:           return array.applyInts(v -> vector.getInt(v.index()));
                case LONG:              return array.applyLongs(v -> vector.getLong(v.index()));
                case DOUBLE:            return array.applyDoubles(v -> vector.getDouble(v.index()));
                case DATE:              return array.applyLongs(v -> vector.getLong(v.index()));
                case INSTANT:           return array.applyLongs(v -> vector.getLong(v.index()));
                case LOCAL_DATE:        return array.applyLongs(v -> vector.getLong(v.index()));
                case LOCAL_TIME:        return array.applyLongs(v -> vector.getLong(v.index()));
                case LOCAL_DATETIME:    return array.applyLongs(v -> vector.getLong(v.index()));
                default:                return array.applyValues(v -> vector.getValue(v.index()));
            }
        }
    }


    /**
     * Returns column data as an array for internal use only
     * @param colKey    the column key
     * @return          the array of column data
     */
    private Array<?> getColArray(C colKey) {
        if (columnStore) {
            final int index = colKeys.getIndexForKey(colKey);
            return data.get(index);
        } else {
            final Class<?> type = colType(colKey);
            final Array<?> array = Array.of(type, rowKeys.size());
            final Vector<C,R> vector = colCursor().moveTo(colKey);
            switch (ArrayType.of(type)) {
                case BOOLEAN:           return array.applyBooleans(v -> vector.getBoolean(v.index()));
                case INTEGER:           return array.applyInts(v -> vector.getInt(v.index()));
                case LONG:              return array.applyLongs(v -> vector.getLong(v.index()));
                case DOUBLE:            return array.applyDoubles(v -> vector.getDouble(v.index()));
                case DATE:              return array.applyLongs(v -> vector.getLong(v.index()));
                case INSTANT:           return array.applyLongs(v -> vector.getLong(v.index()));
                case LOCAL_DATE:        return array.applyLongs(v -> vector.getLong(v.index()));
                case LOCAL_TIME:        return array.applyLongs(v -> vector.getLong(v.index()));
                case LOCAL_DATETIME:    return array.applyLongs(v -> vector.getLong(v.index()));
                default:                return array.applyValues(v -> vector.getValue(v.index()));
            }
        }
    }


    /**
     * Returns a deep copy of this contents
     * @return  a deep copy of this contents
     */
    final XDataFrameContent<R,C> copy() {
        return isColumnStore() ? copyColumnStore() : copyRowStore();
    }


    /**
     * Returns a deep copy of this content which is expressed as a row store
     * @return  the deep copy of this content
     */
    @SuppressWarnings("unchecked")
    private XDataFrameContent<R,C> copyRowStore() {
        try {
            if (colDim().isFilter()) {
                final Array<R> rowKeys = rowDim().toArray();
                final Array<C> colKeys = colDim().toArray();
                final int[] modelIndexes = colDim().indexes().toArray();
                final Index<R> newRowAxis = Index.of(rowKeys);
                final Index<C> newColAxis = Index.of(colKeys);
                final List<Array<?>> newData = rowDim().keys().map(k -> getArray(k).copy(modelIndexes)).collect(Collectors.toList());
                return new XDataFrameContent<>(newRowAxis, newColAxis, columnStore, newData);
            } else if (rowDim().isFilter()) {
                final Array<C> colKeys = colDim().toArray();
                final Index<R> newRowAxis = rowDim().copy();
                final Index<C> newColAxis = Index.of(colKeys);
                final List<Array<?>> newData = rowDim().keys().map(k -> getArray(k).copy()).collect(Collectors.toList());
                return new XDataFrameContent<>(newRowAxis, newColAxis, columnStore, newData);
            } else {
                final XDataFrameContent<R,C> clone = (XDataFrameContent<R,C>)super.clone();
                clone.data = this.data.stream().map(Array::copy).collect(Collectors.toList());
                clone.rowKeys = this.rowKeys.copy();
                clone.colKeys = this.colKeys.copy();
                return clone;
            }
        } catch (CloneNotSupportedException ex) {
            throw new DataFrameException("Clone operation not supported", ex);
        }
    }


    /**
     * Returns a deep copy of this content which is expressed as a column store
     * @return  the deep copy of this content
     */
    @SuppressWarnings("unchecked")
    private XDataFrameContent<R,C> copyColumnStore() {
        try {
            if (rowDim().isFilter()) {
                final Array<R> rowKeys = this.rowKeys.toArray();
                final Array<C> colKeys = this.colKeys.toArray();
                final int[] modelIndexes = this.rowKeys.indexes().toArray();
                final Index<R> newRowAxis = Index.of(rowKeys);
                final Index<C> newColAxis = Index.of(colKeys);
                final List<Array<?>> newData = this.colKeys.keys().map(c -> getArray(c).copy(modelIndexes)).collect(Collectors.toList());
                return new XDataFrameContent<>(newRowAxis, newColAxis, columnStore, newData);
            } else if (colDim().isFilter()) {
                final Array<C> colKeys = this.colKeys.toArray();
                final Index<R> newRowAxis = rowKeys.copy();
                final Index<C> newColAxis = Index.of(colKeys);
                final List<Array<?>> newData = this.colKeys.keys().map(c -> getArray(c).copy()).collect(Collectors.toList());
                return new XDataFrameContent<>(newRowAxis, newColAxis, columnStore, newData);
            } else {
                final XDataFrameContent<R,C> clone = (XDataFrameContent<R,C>)super.clone();
                clone.data = this.data.stream().map(Array::copy).collect(Collectors.toList());
                clone.rowKeys = this.rowKeys.copy();
                clone.colKeys = this.colKeys.copy();
                return clone;
            }
        } catch (CloneNotSupportedException ex) {
            throw new DataFrameException("Clone operation not supported", ex);
        }
    }


    /**
     * Returns direct access to the RowCursor class so calls can be inlined for performance
     * @return  direct reference to the RowCursor class
     */
    final Row rowCursorDirect() {
        return new Row(this);
    }


    /**
     * Returns direct access to the ColumnCursor class so calls can be inlined for performance
     * @return  direct reference to the ColumnCursor class
     */
    final Column colCursorDirect() {
        return new Column(this);
    }


    @Override
    public final Vector<R,C> rowCursor() {
        return new Row(this);
    }


    @Override
    public final Vector<C,R> colCursor() {
        return new Column(this);
    }


    @Override
    public final boolean getBoolean(R rowKey, int colOrdinal) {
        try {
            final int rowIndex = rowKeys.getIndexForKey(rowKey);
            final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.getBoolean(rowIndex);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.getBoolean(colIndex);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + colOrdinal + ")", t);
        }
    }


    @Override
    public final boolean getBoolean(int rowOrdinal, C colKey) {
        try {
            final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
            final int colIndex = colKeys.getIndexForKey(colKey);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.getBoolean(rowIndex);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.getBoolean(colIndex);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowOrdinal + ", " + colKey + ")", t);
        }
    }


    @Override
    public final boolean getBoolean(R rowKey, C colKey) {
        try {
            final int rowIndex = rowKeys.getIndexForKey(rowKey);
            final int colIndex = colKeys.getIndexForKey(colKey);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.getBoolean(rowIndex);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.getBoolean(colIndex);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + colKey + ")", t);
        }
    }


    @Override
    public final boolean getBoolean(int rowOrdinal, int colOrdinal) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
            final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.getBoolean(rowIndex);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.getBoolean(colIndex);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowOrdinal + ", " + colOrdinal + ")", t);
        }
    }


    @Override
    public final int getInt(R rowKey, int colOrdinal) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForKey(rowKey);
            final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.getInt(rowIndex);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.getInt(colIndex);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + colOrdinal + ")", t);
        }
    }


    @Override
    public final int getInt(R rowKey, C colKey) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForKey(rowKey);
            final int colIndex = colKeys.getIndexForKey(colKey);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.getInt(rowIndex);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.getInt(colIndex);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + colKey + ")", t);
        }
    }


    @Override
    public final int getInt(int rowOrdinal, C colKey) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
            final int colIndex = colKeys.getIndexForKey(colKey);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.getInt(rowIndex);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.getInt(colIndex);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowOrdinal + ", " + colKey + ")", t);
        }
    }


    @Override
    public final int getInt(int rowOrdinal, int colOrdinal) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
            final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.getInt(rowIndex);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.getInt(colIndex);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowOrdinal + ", " + colOrdinal + ")", t);
        }
    }


    @Override
    public final long getLong(R rowKey, C colKey) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForKey(rowKey);
            final int colIndex = colKeys.getIndexForKey(colKey);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.getLong(rowIndex);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.getLong(colIndex);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + colKey + ")", t);
        }
    }


    @Override
    public final long getLong(R rowKey, int colOrdinal) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForKey(rowKey);
            final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.getLong(rowIndex);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.getLong(colIndex);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + colOrdinal + ")", t);
        }
    }


    @Override
    public final long getLong(int rowOrdinal, C colKey) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
            final int colIndex = colKeys.getIndexForKey(colKey);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.getLong(rowIndex);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.getLong(colIndex);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowOrdinal + ", " + colKey + ")", t);
        }
    }


    @Override
    public final long getLong(int rowOrdinal, int colOrdinal) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
            final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.getLong(rowIndex);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.getLong(colIndex);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowOrdinal + ", " + colOrdinal + ")", t);
        }
    }


    @Override
    public final double getDouble(R rowKey, C colKey) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForKey(rowKey);
            final int colIndex = colKeys.getIndexForKey(colKey);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.getDouble(rowIndex);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.getDouble(colIndex);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + colKey + ")", t);
        }
    }


    @Override
    public final double getDouble(R rowKey, int colOrdinal) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForKey(rowKey);
            final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.getDouble(rowIndex);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.getDouble(colIndex);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + colOrdinal + ")", t);
        }
    }


    @Override
    public final double getDouble(int rowOrdinal, C colKey) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
            final int colIndex = colKeys.getIndexForKey(colKey);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.getDouble(rowIndex);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.getDouble(colIndex);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowOrdinal + ", " + colKey + ")", t);
        }
    }


    @Override
    public final double getDouble(int rowOrdinal, int colOrdinal) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
            final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.getDouble(rowIndex);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.getDouble(colIndex);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowOrdinal + ", " + colOrdinal + ")", t);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public final <T> T getValue(R rowKey, C colKey) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForKey(rowKey);
            final int colIndex = colKeys.getIndexForKey(colKey);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return (T)colArray.getValue(rowIndex);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return (T)rowArray.getValue(colIndex);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + colKey + ")", t);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public final <T> T getValue(R rowKey, int colOrdinal) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForKey(rowKey);
            final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return (T)colArray.getValue(rowIndex);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return (T)rowArray.getValue(colIndex);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + colOrdinal + ")", t);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public final <T> T getValue(int rowOrdinal, C colKey) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
            final int colIndex = colKeys.getIndexForKey(colKey);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return (T)colArray.getValue(rowIndex);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return (T)rowArray.getValue(colIndex);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowOrdinal + ", " + colKey + ")", t);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public final <T> T getValue(int rowOrdinal, int colOrdinal) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
            final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return (T)colArray.getValue(rowIndex);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return (T)rowArray.getValue(colIndex);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowOrdinal + ", " + colOrdinal + ")", t);
        }
    }


    @Override
    public final boolean setBoolean(R rowKey, C colKey, boolean value) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForKey(rowKey);
            final int colIndex = colKeys.getIndexForKey(colKey);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.setBoolean(rowIndex, value);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.setBoolean(colIndex, value);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + colKey + ")", t);
        }
    }


    @Override
    public final boolean setBoolean(R rowKey, int colOrdinal, boolean value) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForKey(rowKey);
            final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.setBoolean(rowIndex, value);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.setBoolean(colIndex, value);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + colOrdinal + ")", t);
        }
    }


    @Override
    public final boolean setBoolean(int rowOrdinal, C colKey, boolean value) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
            final int colIndex = colKeys.getIndexForKey(colKey);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.setBoolean(rowIndex, value);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.setBoolean(colIndex, value);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowOrdinal + ", " + colKey + ")", t);
        }
    }


    @Override
    public final boolean setBoolean(int rowOrdinal, int colOrdinal, boolean value) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
            final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.setBoolean(rowIndex, value);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.setBoolean(colIndex, value);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowOrdinal + ", " + colOrdinal + ")", t);
        }
    }


    @Override
    public final int setInt(R rowKey, C colKey, int value) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForKey(rowKey);
            final int colIndex = colKeys.getIndexForKey(colKey);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.setInt(rowIndex, value);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.setInt(colIndex, value);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + colKey + ")", t);
        }
    }


    @Override
    public final int setInt(R rowKey, int colOrdinal, int value) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForKey(rowKey);
            final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.setInt(rowIndex, value);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.setInt(colIndex, value);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + colOrdinal + ")", t);
        }
    }


    @Override
    public final int setInt(int rowOrdinal, C colKey, int value) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
            final int colIndex = colKeys.getIndexForKey(colKey);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.setInt(rowIndex, value);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.setInt(colIndex, value);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowOrdinal + ", " + colKey + ")", t);
        }
    }


    @Override
    public final int setInt(int rowOrdinal, int colOrdinal, int value) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
            final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.setInt(rowIndex, value);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.setInt(colIndex, value);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowOrdinal + ", " + colOrdinal + ")", t);
        }
    }


    @Override
    public final long setLong(R rowKey, C colKey, long value) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForKey(rowKey);
            final int colIndex = colKeys.getIndexForKey(colKey);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.setLong(rowIndex, value);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.setLong(colIndex, value);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + colKey + ")", t);
        }
    }


    @Override
    public final long setLong(R rowKey, int colOrdinal, long value) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForKey(rowKey);
            final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.setLong(rowIndex, value);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.setLong(colIndex, value);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + colOrdinal + ")", t);
        }
    }


    @Override
    public final long setLong(int rowOrdinal, C colKey, long value) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
            final int colIndex = colKeys.getIndexForKey(colKey);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.setLong(rowIndex, value);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.setLong(colIndex, value);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowOrdinal + ", " + colKey + ")", t);
        }
    }


    @Override
    public final long setLong(int rowOrdinal, int colOrdinal, long value) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
            final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.setLong(rowIndex, value);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.setLong(colIndex, value);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowOrdinal + ", " + colOrdinal + ")", t);
        }
    }


    @Override
    public final double setDouble(R rowKey, C colKey, double value) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForKey(rowKey);
            final int colIndex = colKeys.getIndexForKey(colKey);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.setDouble(rowIndex, value);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.setDouble(colIndex, value);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + colKey + ")", t);
        }
    }


    @Override
    public final double setDouble(R rowKey, int colOrdinal, double value) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForKey(rowKey);
            final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.setDouble(rowIndex, value);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.setDouble(colIndex, value);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + colOrdinal + ")", t);
        }
    }


    @Override
    public final double setDouble(int rowOrdinal, C colKey, double value) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
            final int colIndex = colKeys.getIndexForKey(colKey);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.setDouble(rowIndex, value);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.setDouble(colIndex, value);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowOrdinal + ", " + colKey + ")", t);
        }
    }


    @Override
    public final double setDouble(int rowOrdinal, int colOrdinal, double value) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
            final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
            if (columnStore) {
                final Array<?> colArray = data.get(colIndex);
                return colArray.setDouble(rowIndex, value);
            } else {
                final Array<?> rowArray = data.get(rowIndex);
                return rowArray.setDouble(colIndex, value);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowOrdinal + ", " + colOrdinal + ")", t);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public final <T> T setValue(R rowKey, C colKey, T value) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForKey(rowKey);
            final int colIndex = colKeys.getIndexForKey(colKey);
            if (columnStore) {
                final Array<Object> colArray = (Array<Object>)data.get(colIndex);
                return (T)colArray.setValue(rowIndex, value);
            } else {
                final Array<Object> rowArray = (Array<Object>)data.get(rowIndex);
                return (T)rowArray.setValue(colIndex, value);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + colKey + ")", t);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public final <T> T setValue(R rowKey, int colOrdinal, T value) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForKey(rowKey);
            final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
            if (columnStore) {
                final Array<T> colArray = (Array<T>)data.get(colIndex);
                return colArray.setValue(rowIndex, value);
            } else {
                final Array<T> rowArray = (Array<T>)data.get(rowIndex);
                return rowArray.setValue(colIndex, value);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + colOrdinal + ")", t);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public final <T> T setValue(int rowOrdinal, C colKey, T value) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
            final int colIndex = colKeys.getIndexForKey(colKey);
            if (columnStore) {
                final Array<T> colArray = (Array<T>)data.get(colIndex);
                return colArray.setValue(rowIndex, value);
            } else {
                final Array<T> rowArray = (Array<T>)data.get(rowIndex);
                return rowArray.setValue(colIndex, value);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowOrdinal + ", " + colKey + ")", t);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public final <T> T setValue(int rowOrdinal, int colOrdinal, T value) throws DataFrameException {
        try {
            final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
            final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
            if (columnStore) {
                final Array<T> colArray = (Array<T>)data.get(colIndex);
                return colArray.setValue(rowIndex, value);
            } else {
                final Array<T> rowArray = (Array<T>)data.get(rowIndex);
                return rowArray.setValue(colIndex, value);
            }
        } catch (Throwable t) {
            throw new DataFrameException("DataFrame access error at (" + rowOrdinal + ", " + colOrdinal + ")", t);
        }
    }


    /**
     * Custom object serialization method for improved performance
     * @param is    the input stream
     * @throws IOException  if read fails
     * @throws ClassNotFoundException   if read fails
     */
    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
        final int rowCount = is.readInt();
        final int colCount = is.readInt();
        final Class<R> rowType = (Class<R>)is.readObject();
        final Class<C> colType = (Class<C>)is.readObject();
        this.columnStore = is.readBoolean();
        this.rowKeys = Index.of(rowType, rowCount);
        this.colKeys = Index.of(colType, colCount);
        if (columnStore) {
            for (int i=0; i<rowCount; ++i) {
                final R rowKey = (R)is.readObject();
                this.rowKeys.add(rowKey);
            }
            this.data = new ArrayList<>(colCount);
            for (int j=0; j<colCount; ++j) {
                final C colKey = (C)is.readObject();
                final Class<?> type = (Class<?>)is.readObject();
                final Array array = Array.of(type, rowCount);
                array.read(is, rowCount);
                this.data.add(array);
                this.colKeys.add(colKey);
            }
        } else {
            for (int i=0; i<colCount; ++i) {
                final C rowKey = (C)is.readObject();
                this.colKeys.add(rowKey);
            }
            this.data = new ArrayList<>(rowCount);
            for (int j=0; j<rowCount; ++j) {
                final R rowKey = (R)is.readObject();
                final Class<?> type = (Class<?>)is.readObject();
                final Array array = Array.of(type, rowCount);
                array.read(is, rowCount);
                this.data.add(array);
                this.rowKeys.add(rowKey);
            }
        }
    }


    /**
     * Custom object serialization method for improved performance
     * @param os    the output stream
     * @throws IOException  if write fails
     */
    private void writeObject(ObjectOutputStream os) throws IOException {
        final int rowCount = rowKeys.size();
        final int colCount = colKeys.size();
        os.writeInt(rowCount);
        os.writeInt(colCount);
        os.writeObject(rowKeys.type());
        os.writeObject(colKeys.type());
        os.writeBoolean(columnStore);
        if (isColumnStore()) {
            for (int i=0; i<rowCount; ++i) {
                final R rowKey = rowKeys.getKey(i);
                os.writeObject(rowKey);
            }
            final int[] indexes = rowKeys.indexes().toArray();
            for (int j=0; j<colCount; ++j) {
                final C colKey = colDim().getKey(j);
                final Array<?> array = data.get(j);
                final Class<?> type = array.type();
                os.writeObject(colKey);
                os.writeObject(type);
                array.write(os, indexes);
            }
        } else {
            for (int i=0; i<colCount; ++i) {
                final C colKey = colKeys.getKey(i);
                os.writeObject(colKey);
            }
            final int[] indexes = colKeys.indexes().toArray();
            for (int j=0; j<rowCount; ++j) {
                final R rowKey = rowKeys.getKey(j);
                final Array<?> array = data.get(j);
                final Class<?> type = array.type();
                os.writeObject(rowKey);
                os.writeObject(type);
                array.write(os, indexes);
            }
        }
    }


    /**
     * A DataFrameCursor implementation that operates in view space
     */
    private class Cursor implements DataFrameCursor<R,C> {

        private int rowIndex;
        private int colIndex;
        private int rowOrdinal;
        private int colOrdinal;
        private Array<?> array;
        private XDataFrame<R,C> frame;
        private XDataFrameRow<R,C> row;
        private XDataFrameColumn<R,C> column;

        /**
         * Constructor
         * @param frame         the frame reference
         * @param rowOrdinal    the initial row ordinal
         * @param colOrdinal    the initial column ordinal
         */
        private Cursor(XDataFrame<R,C> frame, int rowOrdinal, int colOrdinal) {
            this.frame = frame;
            if (rowOrdinal >= 0) moveToRow(rowOrdinal);
            if (colOrdinal >= 0) moveToColumn(colOrdinal);
        }

        @Override
        public final R rowKey() {
            return rowOrdinal < 0 ? null : rowKeys.getKey(rowOrdinal);
        }

        @Override
        public final C colKey() {
            return colOrdinal < 0 ? null : colKeys.getKey(colOrdinal);
        }

        @Override
        public final int rowOrdinal() {
            return rowOrdinal;
        }

        @Override
        public final int colOrdinal() {
            return colOrdinal;
        }

        @Override
        public final DataFrame<R,C> frame() {
            return frame;
        }

        @Override
        public final boolean isBoolean() {
            return array.typeCode() == ArrayType.BOOLEAN;
        }

        @Override
        public final boolean isInteger() {
            return array.typeCode() == ArrayType.INTEGER;
        }

        @Override
        public final boolean isLong() {
            return array.typeCode() == ArrayType.LONG;
        }

        @Override
        public final boolean isDouble() {
            return array.typeCode() == ArrayType.DOUBLE;
        }

        @Override
        public final boolean isNull() {
            try {
                if (columnStore) {
                    return array.isNull(rowIndex);
                } else {
                    return array.isNull(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame read error at (" + rowKey() + ", " + colKey() + "): " + ex.getMessage(), ex);
            }
        }

        @Override()
        public final boolean isEqualTo(Object value) {
            if (value instanceof Number) {
                switch (array.typeCode()) {
                    case INTEGER:   return ((Number)value).intValue() == getInt();
                    case LONG:      return ((Number)value).longValue() == getLong();
                    case DOUBLE:
                        final double value1 = getDouble();
                        final double value2 = ((Number)value).doubleValue();
                        return Double.compare(value1, value2) == 0;
                }
            }
            final Object thisValue = getValue();
            return Objects.equals(value, thisValue);
        }

        @Override
        public final DataFrameRow<R,C> row() {
            if (row == null) {
                this.row = new XDataFrameRow<>(frame, false, rowOrdinal);
                return row;
            } else {
                this.row.moveTo(rowOrdinal);
                return row;
            }
        }

        @Override
        public final DataFrameColumn<R,C> col() {
            if (column == null) {
                this.column = new XDataFrameColumn<>(frame, false, colOrdinal);
                return column;
            } else {
                this.column.moveTo(colOrdinal);
                return column;
            }
        }

        @Override
        public final boolean getBoolean() {
            try {
                if (columnStore) {
                    return array.getBoolean(rowIndex);
                } else {
                    return array.getBoolean(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame read error at (" + rowKey() + ", " + colKey() + "): " + ex.getMessage(), ex);
            }
        }

        @Override
        public final int getInt() {
            try {
                if (columnStore) {
                    return array.getInt(rowIndex);
                } else {
                    return array.getInt(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame read error at (" + rowKey() + ", " + colKey() + "): " + ex.getMessage(), ex);
            }
        }

        @Override
        public final long getLong() {
            try {
                if (columnStore) {
                    return array.getLong(rowIndex);
                } else {
                    return array.getLong(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame read error at (" + rowKey() + ", " + colKey() + "): " + ex.getMessage(), ex);
            }
        }

        @Override
        public final double getDouble() {
            try {
                if (columnStore) {
                    return array.getDouble(rowIndex);
                } else {
                    return array.getDouble(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame read error at (" + rowKey() + ", " + colKey() + "): " + ex.getMessage(), ex);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public final <V> V getValue() {
            try {
                if (columnStore) {
                    return (V)array.getValue(rowIndex);
                } else {
                    return (V)array.getValue(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame read error at (" + rowKey() + ", " + colKey() + "): " + ex.getMessage(), ex);
            }
        }

        @Override
        public final void setBoolean(boolean value) {
            try {
                if (columnStore) {
                    array.setBoolean(rowIndex, value);
                } else {
                    array.setBoolean(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame write error at (" + rowKey() + ", " + colKey() + "): " + ex.getMessage(), ex);
            }
        }

        @Override
        public final void setInt(int value) {
            try {
                if (columnStore) {
                    array.setInt(rowIndex, value);
                } else {
                    array.setInt(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame write error at (" + rowKey() + ", " + colKey() + "): " + ex.getMessage(), ex);
            }
        }

        @Override
        public final void setLong(long value) {
            try {
                if (columnStore) {
                    array.setLong(rowIndex, value);
                } else {
                    array.setLong(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame write error at (" + rowKey() + ", " + colKey() + "): " + ex.getMessage(), ex);
            }
        }

        @Override
        public final void setDouble(double value) {
            try {
                if (columnStore) {
                    array.setDouble(rowIndex, value);
                } else {
                    array.setDouble(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame write error at (" + rowKey() + ", " + colKey() + "): " + ex.getMessage(), ex);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public final <V> void setValue(V value) {
            try {
                if (columnStore) {
                    ((Array<V>)array).setValue(rowIndex, value);
                } else {
                    ((Array<V>)array).setValue(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame write error at (" + rowKey() + ", " + colKey() + "): " + ex.getMessage(), ex);
            }
        }

        @Override
        public final DataFrameCursor<R,C> moveToRow(int rowOrdinal) {
            this.rowOrdinal = rowOrdinal;
            this.rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
            this.array = columnStore ? array : data.get(rowIndex);
            return this;
        }

        @Override
        public final DataFrameCursor<R,C> moveToColumn(int colOrdinal) {
            this.colOrdinal = colOrdinal;
            this.colIndex = colKeys.getIndexForOrdinal(colOrdinal);
            this.array = columnStore ? data.get(colIndex) : array;
            return this;
        }

        @Override
        public final DataFrameCursor<R,C> copy() {
            return new Cursor(frame, rowOrdinal, colOrdinal);
        }

        @Override
        public DataFrameCursor<R,C> moveTo(DataFrameValue value) {
            return moveTo(value.rowOrdinal(), value.colOrdinal());
        }

        @Override
        public final DataFrameCursor<R,C> moveToRow(R rowKey) {
            return moveToRow(rowKeys.getOrdinalForKey(rowKey));
        }

        @Override
        public final DataFrameCursor<R,C> moveToColumn(C colKey) {
            return moveToColumn(colKeys.getOrdinalForKey(colKey));
        }

        @Override
        public final DataFrameCursor<R,C> moveTo(R rowKey, C colKey) {
            return moveToRow(rowKey).moveToColumn(colKey);
        }

        @Override
        public final DataFrameCursor<R,C> moveTo(int rowOrdinal, C colKey) {
            return moveToRow(rowOrdinal).moveToColumn(colKey);
        }

        @Override
        public final DataFrameCursor<R,C> moveTo(R rowKey, int colOrdinal) {
            return moveToRow(rowKey).moveToColumn(colOrdinal);
        }

        @Override
        public final DataFrameCursor<R,C> moveTo(int rowOrdinal, int colOrdinal) {
            return moveToRow(rowOrdinal).moveToColumn(colOrdinal);
        }

        @Override
        @SuppressWarnings("unchecked")
        public int compareTo(DataFrameValue<R,C> other) {
            final Object v1 = getValue();
            final Object v2 = other.getValue();
            try {
                if (v1 == v2) {
                    return 0;
                } else if (v1 == null) {
                    return -1;
                } else if (v2 == null) {
                    return 1;
                } else if (v1.getClass() == v2.getClass()) {
                    if (v1 instanceof Comparable) {
                        final Comparable c1 = (Comparable)v1;
                        final Comparable c2 = (Comparable)v2;
                        return c1.compareTo(c2);
                    } else {
                        final String s1 = v1.toString();
                        final String s2 = v2.toString();
                        return s1.compareTo(s2);
                    }
                } else {
                    return 0;
                }
            } catch (Exception ex) {
                throw new DataFrameException("Failed to compare DataFrameValues: " + v1 + " vs " + v2);
            }
        }
    }




    /**
     * A class that implements a movable row vector on this content
     */
    class Row implements Vector<R,C> {

        private int rowIndex = -1;
        private int rowOrdinal = -1;
        private Array<?> rowArray;
        private XDataFrameContent<R,C> content;

        /**
         * Constructor
         * @param content   the content to operate on
         */
        private Row(XDataFrameContent<R,C> content) {
            this.content = content;
        }

        @Override()
        public final boolean isNumeric() {
            return ArrayType.of(typeInfo()).isNumeric();
        }

        @Override()
        public final R key() {
            return rowKeys.getKey(rowOrdinal);
        }

        @Override()
        public final int ordinal() {
            return rowOrdinal;
        }

        @Override()
        public final Vector<R,C> moveTo(R rowKey) {
            this.rowIndex = rowKeys.getIndexForKey(rowKey);
            this.rowOrdinal = rowKeys.getOrdinalForKey(rowKey);
            this.rowArray = columnStore ? null : data.get(rowIndex);
            return this;
        }

        @Override()
        public final Vector<R,C> moveTo(int rowOrdinal) {
            if (this.rowOrdinal != rowOrdinal) {
                this.rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
                this.rowOrdinal = rowOrdinal;
                this.rowArray = columnStore ? null : data.get(rowIndex);
            }
            return this;
        }

        @Override
        public Class<?> typeInfo() {
            if (rowArray != null) {
                return rowArray.type();
            } else {
                return content.typeInfo();
            }
        }

        @Override()
        public final boolean getBoolean(C colKey) {
            try {
                final int colIndex = colKeys.getIndexForKey(colKey);
                if (columnStore) {
                    final Array<?> colArray = data.get(colIndex);
                    return colArray.getBoolean(rowIndex);
                } else {
                    return rowArray.getBoolean(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + key() + ", " + colKey + ")", ex);
            }
        }

        @Override()
        public final boolean getBoolean(int colOrdinal) {
            try {
                final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
                if (columnStore) {
                    final Array<?> array = data.get(colIndex);
                    return array.getBoolean(rowIndex);
                } else {
                    return rowArray.getBoolean(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + key() + ", " + colKeys.getKey(colOrdinal) + ")", ex);
            }
        }

        @Override()
        public final int getInt(C colKey) {
            try {
                final int colIndex = colKeys.getIndexForKey(colKey);
                if (columnStore) {
                    final Array<?> colArray = data.get(colIndex);
                    return colArray.getInt(rowIndex);
                } else {
                    return rowArray.getInt(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + key() + ", " + colKey + ")", ex);
            }
        }

        @Override()
        public final int getInt(int colOrdinal) {
            try {
                final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
                if (columnStore) {
                    final Array<?> array = data.get(colIndex);
                    return array.getInt(rowIndex);
                } else {
                    return rowArray.getInt(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + key() + ", " + colKeys.getKey(colOrdinal) + ")", ex);
            }
        }

        @Override()
        public final long getLong(C colKey) {
            try {
                final int colIndex = colKeys.getIndexForKey(colKey);
                if (columnStore) {
                    final Array<?> colArray = data.get(colIndex);
                    return colArray.getLong(rowIndex);
                } else {
                    return rowArray.getLong(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + key() + ", " + colKey + ")", ex);
            }
        }

        @Override()
        public final long getLong(int colOrdinal) {
            try {
                final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
                if (columnStore) {
                    final Array<?> array = data.get(colIndex);
                    return array.getLong(rowIndex);
                } else {
                    return rowArray.getLong(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + key() + ", " + colKeys.getKey(colOrdinal) + ")", ex);
            }
        }

        @Override()
        public final double getDouble(C colKey) {
            try {
                final int colIndex = colKeys.getIndexForKey(colKey);
                if (columnStore) {
                    final Array<?> colArray = data.get(colIndex);
                    return colArray.getDouble(rowIndex);
                } else {
                    return rowArray.getDouble(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + key() + ", " + colKey + ")", ex);
            }
        }

        @Override()
        public final double getDouble(int colOrdinal) {
            try {
                final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
                if (columnStore) {
                    final Array<?> array = data.get(colIndex);
                    return array.getDouble(rowIndex);
                } else {
                    return rowArray.getDouble(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + key() + ", " + colKeys.getKey(colOrdinal) + ")", ex);
            }
        }

        @Override()
        @SuppressWarnings("unchecked")
        public final <V> V getValue(C colKey) {
            try {
                final int colIndex = colKeys.getIndexForKey(colKey);
                if (columnStore) {
                    final Array<?> colArray = data.get(colIndex);
                    return (V)colArray.getValue(rowIndex);
                } else {
                    return (V)rowArray.getValue(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + key() + ", " + colKey + ")", ex);
            }
        }

        @Override()
        @SuppressWarnings("unchecked")
        public final <V> V getValue(int colOrdinal) {
            try {
                final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
                if (columnStore) {
                    final Array<?> array = data.get(colIndex);
                    return (V)array.getValue(rowIndex);
                } else {
                    return (V)rowArray.getValue(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + key() + ", " + colKeys.getKey(colOrdinal) + ")", ex);
            }
        }

        @Override()
        public final boolean setBoolean(C colKey, boolean value) {
            try {
                final int colIndex = colKeys.getIndexForKey(colKey);
                if (columnStore) {
                    final Array<?> colArray = data.get(colIndex);
                    return colArray.setBoolean(rowIndex, value);
                } else {
                    return rowArray.setBoolean(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + key() + ", " + colKey + ")", ex);
            }
        }

        @Override()
        public final boolean setBoolean(int colOrdinal, boolean value) {
            try {
                final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
                if (columnStore) {
                    final Array<?> colArray = data.get(colOrdinal);
                    return colArray.setBoolean(rowIndex, value);
                } else {
                    return rowArray.setBoolean(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + key() + ", " + colKeys.getKey(colOrdinal) + ")", ex);
            }
        }

        @Override()
        public final int setInt(C colKey, int value) {
            try {
                final int colIndex = colKeys.getIndexForKey(colKey);
                if (columnStore) {
                    final Array<?> colArray = data.get(colIndex);
                    return colArray.setInt(rowIndex, value);
                } else {
                    return rowArray.setInt(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + key() + ", " + colKey + ")", ex);
            }
        }

        @Override()
        public final int setInt(int colOrdinal, int value) {
            try {
                final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
                if (columnStore) {
                    final Array<?> colArray = data.get(colOrdinal);
                    return colArray.setInt(rowIndex, value);
                } else {
                    return rowArray.setInt(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + key() + ", " + colKeys.getKey(colOrdinal) + ")", ex);
            }
        }

        @Override()
        public final long setLong(C colKey, long value) {
            try {
                final int colIndex = colKeys.getIndexForKey(colKey);
                if (columnStore) {
                    final Array<?> colArray = data.get(colIndex);
                    return colArray.setLong(rowIndex, value);
                } else {
                    return rowArray.setLong(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + key() + ", " + colKey + ")", ex);
            }
        }

        @Override()
        public final long setLong(int colOrdinal, long value) {
            try {
                final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
                if (columnStore) {
                    final Array<?> colArray = data.get(colOrdinal);
                    return colArray.setLong(rowIndex, value);
                } else {
                    return rowArray.setLong(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + key() + ", " + colKeys.getKey(colOrdinal) + ")", ex);
            }
        }

        @Override()
        public final double setDouble(C colKey, double value) {
            try {
                final int colIndex = colKeys.getIndexForKey(colKey);
                if (columnStore) {
                    final Array<?> colArray = data.get(colIndex);
                    return colArray.setDouble(rowIndex, value);
                } else {
                    return rowArray.setDouble(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + key() + ", " + colKey + ")", ex);
            }
        }

        @Override()
        public final double setDouble(int colOrdinal, double value) {
            try {
                final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
                if (columnStore) {
                    final Array<?> colArray = data.get(colOrdinal);
                    return colArray.setDouble(rowIndex, value);
                } else {
                    return rowArray.setDouble(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + key() + ", " + colKeys.getKey(colOrdinal) + ")", ex);
            }
        }

        @Override()
        @SuppressWarnings("unchecked")
        public final <V> V setValue(C colKey, V value) {
            try {
                final int colIndex = colKeys.getIndexForKey(colKey);
                if (columnStore) {
                    final Array<V> colArray = (Array<V>)data.get(colIndex);
                    return colArray.setValue(rowIndex, value);
                } else {
                    return ((Array<V>)rowArray).setValue(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + key() + ", " + colKey + ")", ex);
            }
        }

        @Override()
        @SuppressWarnings("unchecked")
        public final <V> V setValue(int colOrdinal, V value) {
            try {
                final int colIndex = colKeys.getIndexForOrdinal(colOrdinal);
                if (columnStore) {
                    final Array<V> colArray = (Array<V>)data.get(colOrdinal);
                    return colArray.setValue(rowIndex, value);
                } else {
                    return ((Array<V>)rowArray).setValue(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + key() + ", " + colKeys.getKey(colOrdinal) + ")", ex);
            }
        }
    }




    /**
     * A class that implements a movable column vector on this content
     */
    class Column implements Vector<C,R> {

        private int colIndex = -1;
        private int colOrdinal = -1;
        private Array<?> colArray;
        private XDataFrameContent<R,C> content;

        /**
         * Constructor
         * @param content   the content to operate on
         */
        private Column(XDataFrameContent<R,C> content) {
            this.content = content;
        }

        @Override()
        public final boolean isNumeric() {
            return ArrayType.of(typeInfo()).isNumeric();
        }

        @Override()
        public final C key() {
            return colKeys.getKey(colOrdinal);
        }

        @Override()
        public final int ordinal() {
            return colOrdinal;
        }

        @Override()
        public final Class<?> typeInfo() {
            if (colArray != null) {
                return colArray.type();
            } else {
                return content.typeInfo();
            }
        }

        @Override()
        public final Vector<C,R> moveTo(C colKey) {
            this.colIndex = colKeys.getIndexForKey(colKey);
            this.colOrdinal = colKeys.getOrdinalForKey(colKey);
            this.colArray = columnStore ? data.get(colIndex) : null;
            return this;
        }

        @Override()
        public final Vector<C,R> moveTo(int colOrdinal) {
            if (this.colOrdinal != colOrdinal) {
                this.colIndex = colKeys.getIndexForOrdinal(colOrdinal);
                this.colOrdinal = colOrdinal;
                this.colArray = columnStore ? data.get(colIndex) : null;
            }
            return this;
        }

        @Override()
        public final boolean getBoolean(R rowKey) {
            try {
                final int rowIndex = rowKeys.getIndexForKey(rowKey);
                if (columnStore) {
                    return colArray.getBoolean(rowIndex);
                } else {
                    final Array<?> rowArray = data.get(rowIndex);
                    return rowArray.getBoolean(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + key() + "): " + ex.getMessage(), ex);
            }
        }

        @Override()
        public final boolean getBoolean(int rowOrdinal) {
            try {
                final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
                if (columnStore) {
                    return colArray.getBoolean(rowIndex);
                } else {
                    final Array<?> rowArray = data.get(rowIndex);
                    return rowArray.getBoolean(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + rowKeys.getKey(rowOrdinal) + ", " + key() + "): " + ex.getMessage(), ex);
            }
        }

        @Override()
        public final int getInt(R rowKey) {
            try {
                final int rowIndex = rowKeys.getIndexForKey(rowKey);
                if (columnStore) {
                    return colArray.getInt(rowIndex);
                } else {
                    final Array<?> rowArray = data.get(rowIndex);
                    return rowArray.getInt(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + key() + "): " + ex.getMessage(), ex);
            }
        }

        @Override()
        public final int getInt(int rowOrdinal) {
            try {
                final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
                if (columnStore) {
                    return colArray.getInt(rowIndex);
                } else {
                    final Array<?> rowArray = data.get(rowIndex);
                    return rowArray.getInt(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + rowKeys.getKey(rowOrdinal) + ", " + key() + "): " + ex.getMessage(), ex);
            }
        }

        @Override()
        public final long getLong(R rowKey) {
            try {
                final int rowIndex = rowKeys.getIndexForKey(rowKey);
                if (columnStore) {
                    return colArray.getLong(rowIndex);
                } else {
                    final Array<?> rowArray = data.get(rowIndex);
                    return rowArray.getLong(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + key() + "): " + ex.getMessage(), ex);
            }
        }

        @Override()
        public final long getLong(int rowOrdinal) {
            try {
                final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
                if (columnStore) {
                    return colArray.getLong(rowIndex);
                } else {
                    final Array<?> rowArray = data.get(rowIndex);
                    return rowArray.getLong(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + rowKeys.getKey(rowOrdinal) + ", " + key() + "): " + ex.getMessage(), ex);
            }
        }

        @Override()
        public final double getDouble(R rowKey) {
            try {
                final int rowIndex = rowKeys.getIndexForKey(rowKey);
                if (columnStore) {
                    return colArray.getDouble(rowIndex);
                } else {
                    final Array<?> rowArray = data.get(rowIndex);
                    return rowArray.getDouble(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + key() + "): " + ex.getMessage(), ex);
            }
        }

        @Override()
        public final double getDouble(int rowOrdinal) {
            try {
                final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
                if (columnStore) {
                    return colArray.getDouble(rowIndex);
                } else {
                    final Array<?> rowArray = data.get(rowIndex);
                    return rowArray.getDouble(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + rowKeys.getKey(rowOrdinal) + ", " + key() + "): " + ex.getMessage(), ex);
            }
        }

        @Override()
        @SuppressWarnings("unchecked")
        public final <V> V getValue(R rowKey) {
            try {
                final int rowIndex = rowKeys.getIndexForKey(rowKey);
                if (columnStore) {
                    return (V)colArray.getValue(rowIndex);
                } else {
                    final Array<?> rowArray = data.get(rowIndex);
                    return (V)rowArray.getValue(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + key() + "): " + ex.getMessage(), ex);
            }
        }

        @Override()
        @SuppressWarnings("unchecked")
        public final <V> V getValue(int rowOrdinal) {
            try {
                final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
                if (columnStore) {
                    return (V)colArray.getValue(rowIndex);
                } else {
                    final Array<?> rowArray = data.get(rowIndex);
                    return (V)rowArray.getValue(colIndex);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + rowKeys.getKey(rowOrdinal) + ", " + key() + "): " + ex.getMessage(), ex);
            }
        }

        @Override()
        public final boolean setBoolean(R rowKey, boolean value) {
            try {
                final int rowIndex = rowKeys.getIndexForKey(rowKey);
                if (columnStore) {
                    return colArray.setBoolean(rowIndex, value);
                } else {
                    final Array<?> rowArray = data.get(rowIndex);
                    return rowArray.setBoolean(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + key() + "): " + ex.getMessage(), ex);
            }
        }

        @Override()
        public final boolean setBoolean(int rowOrdinal, boolean value) {
            try {
                final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
                if (columnStore) {
                    return colArray.setBoolean(rowIndex, value);
                } else {
                    final Array<?> rowArray = data.get(rowIndex);
                    return rowArray.setBoolean(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + rowKeys.getKey(rowOrdinal) + ", " + key() + "): " + ex.getMessage(), ex);
            }
        }

        @Override()
        public final int setInt(R rowKey, int value) {
            try {
                final int rowIndex = rowKeys.getIndexForKey(rowKey);
                if (columnStore) {
                    return colArray.setInt(rowIndex, value);
                } else {
                    final Array<?> rowArray = data.get(rowIndex);
                    return rowArray.setInt(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + key() + "): " + ex.getMessage(), ex);
            }
        }

        @Override()
        public final int setInt(int rowOrdinal, int value) {
            try {
                final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
                if (columnStore) {
                    return colArray.setInt(rowIndex, value);
                } else {
                    final Array<?> rowArray = data.get(rowIndex);
                    return rowArray.setInt(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + rowKeys.getKey(rowOrdinal) + ", " + key() + "): " + ex.getMessage(), ex);
            }
        }

        @Override()
        public final long setLong(R rowKey, long value) {
            try {
                final int rowIndex = rowKeys.getIndexForKey(rowKey);
                if (columnStore) {
                    return colArray.setLong(rowIndex, value);
                } else {
                    final Array<?> rowArray = data.get(rowIndex);
                    return rowArray.setLong(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + key() + "): " + ex.getMessage(), ex);
            }
        }

        @Override()
        public final long setLong(int rowOrdinal, long value) {
            try {
                final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
                if (columnStore) {
                    return colArray.setLong(rowIndex, value);
                } else {
                    final Array<?> rowArray = data.get(rowIndex);
                    return rowArray.setLong(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + rowKeys.getKey(rowOrdinal) + ", " + key() + "): " + ex.getMessage(), ex);
            }
        }

        @Override()
        public final double setDouble(R rowKey, double value) {
            try {
                final int rowIndex = rowKeys.getIndexForKey(rowKey);
                if (columnStore) {
                    return colArray.setDouble(rowIndex, value);
                } else {
                    final Array<?> rowArray = data.get(rowIndex);
                    return rowArray.setDouble(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + key() + "): " + ex.getMessage(), ex);
            }
        }

        @Override()
        public final double setDouble(int rowOrdinal, double value) {
            try {
                final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
                if (columnStore) {
                    return colArray.setDouble(rowIndex, value);
                } else {
                    final Array<?> rowArray = data.get(rowIndex);
                    return rowArray.setDouble(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + rowKeys.getKey(rowOrdinal) + ", " + key() + "): " + ex.getMessage(), ex);
            }
        }

        @Override()
        @SuppressWarnings("unchecked")
        public final <V> V setValue(R rowKey, V value) {
            try {
                final int rowIndex = rowKeys.getIndexForKey(rowKey);
                if (columnStore) {
                    return ((Array<V>)colArray).setValue(rowIndex, value);
                } else {
                    final Array<V> rowArray = (Array<V>)data.get(rowIndex);
                    return rowArray.setValue(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + rowKey + ", " + key() + "): " + ex.getMessage(), ex);
            }
        }

        @Override()
        @SuppressWarnings("unchecked")
        public final <V> V setValue(int rowOrdinal, V value) {
            try {
                final int rowIndex = rowKeys.getIndexForOrdinal(rowOrdinal);
                if (columnStore) {
                    return ((Array<V>)colArray).setValue(rowIndex, value);
                } else {
                    final Array<V> rowArray = (Array<V>)data.get(rowIndex);
                    return rowArray.setValue(colIndex, value);
                }
            } catch (Exception ex) {
                throw new DataFrameException("DataFrame access error at (" + rowKeys.getKey(rowOrdinal) + ", " + key() + "): " + ex.getMessage(), ex);
            }
        }
    }


}
