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
package com.zavtech.morpheus.frame;

import com.zavtech.morpheus.array.Array;

/**
 * An event descriptor used to describe matrix data change events.
 *
 * <p>This is one of the few classes in the Morpheus library that is defined as a concrete
 * class as opposed to an interface because it's not clear how implementations could
 * vary. This is a simple data container that describes a data change event in a
 * <code>DataFrame</code> and it is consumed by registered <code>DataFrameListeners</code>.
 * The <code>DataFrameEvent</code> provides a number of convenient constructors to
 * signal various types of data change events ranging from single element events
 * to entire row/column vector updates to an entire matrix update.
 * </p>
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public class DataFrameEvent<R,C> implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    public enum Type {ADD, UPDATE, REMOVE}

    private Type type;
    private Array<R> rowKeys;
    private Array<C> colKeys;
    private DataFrame<R,C> frame;

    /**
     * Constructor
     * @param frame     the <code>DataFrame</code> reference
     * @param type      the event type
     * @param rowKeys   the row keys for event
     * @param colKeys   the column keys for event
     */
    public DataFrameEvent(DataFrame<R,C> frame, Type type, Array<R> rowKeys, Array<C> colKeys) {
        this.frame = frame;
        this.type = type;
        this.rowKeys = rowKeys != null ? rowKeys : Array.empty(frame.rows().keyType());
        this.colKeys = colKeys != null ? colKeys : Array.empty(frame.cols().keyType());
    }

    /**
     * Returns a new event to signal that one or more rows have been added
     * @param frame     the matrix reference
     * @param rows      the row keys
     * @return          the newly created event
     */
    public static <X,Y> DataFrameEvent<X,Y> createRowAdd(DataFrame<X,Y> frame, Array<X> rows) {
        return new DataFrameEvent<>(frame, Type.ADD, rows, Array.empty(frame.cols().keyType()));
    }

    /**
     * Returns a new event to signal that one or more rows have been removed
     * @param frame     the matrix reference
     * @param rows      the row keys
     * @return          the newly created event
     */
    public static <X,Y> DataFrameEvent<X,Y> createRowRemove(DataFrame<X,Y> frame, Array<X> rows) {
        return new DataFrameEvent<>(frame, Type.REMOVE, rows, Array.empty(frame.cols().keyType()));
    }

    /**
     * Returns a new event to signal that one or more columns have been added
     * @param frame     the matrix reference
     * @return          the newly created event
     */
    public static <X,Y> DataFrameEvent<X,Y> createColumnAdd(DataFrame<X,Y> frame, Array<Y> columns) {
        return new DataFrameEvent<>(frame, Type.ADD, Array.empty(frame.rows().keyType()), columns);
    }

    /**
     * Returns a new event to signal that one or more columns have been removed
     * @param frame     the matrix reference
     * @return          the newly created event
     */
    public static <X,Y> DataFrameEvent<X,Y> createColumnRemove(DataFrame<X,Y> frame, Array<Y> columns) {
        return new DataFrameEvent<>(frame, Type.REMOVE, Array.empty(frame.rows().keyType()), columns);
    }
    /**
     * Returns a new event to signal that a single cell has updated
     * @param frame     the matrix reference
     * @param row       the row key
     * @param column    the column key
     * @return          the newly created event
     */
    public static <X,Y> DataFrameEvent<X,Y> createUpdateEvent(DataFrame<X,Y> frame, X row, Y column) {
        return new DataFrameEvent<>(frame, Type.UPDATE, Array.singleton(row), Array.singleton(column));
    }

    /**
     * Returns a new event to signal that multiple cells have updated
     * @param frame    the matrix reference
     * @param rows      the row keys
     * @param columns   the column keys
     * @return          the newly created event
     */
    public static <X,Y> DataFrameEvent<X,Y> createUpdateEvent(DataFrame<X,Y> frame, Array<X> rows, Array<Y> columns) {
        return new DataFrameEvent<>(frame, Type.UPDATE, rows, columns);
    }

    /**
     * Returns the matrix associated with this event
     * @return  the matrix reference
     */
    public final DataFrame<R,C> frame() {
        return frame;
    }

    /**
     * Returns the type code for this event
     * @return  event type code (ADD | UPDATE | REMOVE)
     */
    public final Type type() {
        return type;
    }

    /**
     * Returns the row keys for this event
     * @return  the row keys
     */
    public final Array<R> rowKeys() {
        return rowKeys;
    }

    /**
     * Returns the column keys for this event
     * @return  the column keys
     */
    public final Array<C> colKeys() {
        return colKeys;
    }

    /**
     * Returns true if this is a remove event
     * @return  true if remove event
     */
    public final boolean isRemoveEvent() {
        return type == Type.REMOVE;
    }

    /**
     * Returns true if this is a matrix data event
     * @return  true if matrix data event
     */
    public final boolean isDataEvent() {
        return type == Type.UPDATE;
    }

    /**
     * Returns true if this is a matrix structure change event
     * @return  true if matrix structure event
     */
    public final boolean isStructureEvent() {
        return !isDataEvent();
    }

    /**
     * Returns true if this is a single row event
     * @return      true if single row event
     */
    public final boolean isSingleRow() {
        return rowKeys.length() == 1;
    }

    /**
     * Returns true if this is a single column event
     * @return      true if single column event
     */
    public final boolean isSingleColumn() {
        return colKeys.length() == 1;
    }

    /**
     * Returns true if this is a single element event
     * @return  true if single element event
     */
    public final boolean isSingleElement() {
        return isSingleRow() && isSingleColumn();
    }
}
