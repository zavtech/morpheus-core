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

/**
 * An interface that provides a random access API to read/write individual elements of a DataFrame.
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameContent<R,C> {

    /**
     * Returns a newly created light-weight row cursor on this content
     * A Vector is movable and is therefore not thread safe so concurrent access needs to be managed.
     * @return  the newly created row cursor
     */
    Vector<R,C> rowCursor();

    /**
     * Returns a newly created light-weight column cursor on this content
     * A Vector is movable and is therefore not thread safe so concurrent access needs to be managed.
     * @return  the newly created column cursor
     */
    Vector<C,R> colCursor();

    /**
     * Returns the value for the coordinates specified
     * @param rowIndex  the row index coordinate
     * @param colIndex  the column index coordinate
     * @return  the value for coordinates
     */
    boolean getBoolean(int rowIndex, int colIndex);

    /**
     * Returns the value for the coordinates specified
     * @param row       the row key coordinate
     * @param column    the column key coordinate
     * @return  the value for coordinates
     */
    boolean getBoolean(R row, C column);

    /**
     * Returns the value for the coordinates specified
     * @param row       the row key coordinate
     * @param colIndex  the column index coordinate
     * @return  the value for coordinates
     */
    boolean getBoolean(R row, int colIndex);

    /**
     * Returns the value for the coordinates specified
     * @param rowIndex  the row index coordinate
     * @param column    the column key coordinate
     * @return  the value for coordinates
     */
    boolean getBoolean(int rowIndex, C column);

    /**
     * Returns the value for the coordinates specified
     * @param rowIndex  the row index coordinate
     * @param colIndex  the column index coordinate
     * @return  the value for coordinates
     */
    int getInt(int rowIndex, int colIndex);

    /**
     * Returns the value for the coordinates specified
     * @param row       the row key coordinate
     * @param column    the column key coordinate
     * @return  the value for coordinates
     */
    int getInt(R row, C column);

    /**
     * Returns the value for the coordinates specified
     * @param row       the row key coordinate
     * @param colIndex  the column index coordinate
     * @return  the value for coordinates
     */
    int getInt(R row, int colIndex);

    /**
     * Returns the value for the coordinates specified
     * @param rowIndex  the row index coordinate
     * @param column    the column key coordinate
     * @return  the value for coordinates
     */
    int getInt(int rowIndex, C column);

    /**
     * Returns the value for the coordinates specified
     * @param rowIndex  the row index coordinate
     * @param colIndex  the column index coordinate
     * @return  the value for coordinates
     */
    long getLong(int rowIndex, int colIndex);

    /**
     * Returns the value for the coordinates specified
     * @param row       the row key coordinate
     * @param column    the column key coordinate
     * @return  the value for coordinates
     */
    long getLong(R row, C column);

    /**
     * Returns the value for the coordinates specified
     * @param row       the row key coordinate
     * @param colIndex  the column index coordinate
     * @return  the value for coordinates
     */
    long getLong(R row, int colIndex);

    /**
     * Returns the value for the coordinates specified
     * @param rowIndex  the row index coordinate
     * @param column    the column key coordinate
     * @return  the value for coordinates
     */
    long getLong(int rowIndex, C column);

    /**
     * Returns the value for the coordinates specified
     * @param rowIndex  the row index coordinate
     * @param colIndex  the column index coordinate
     * @return  the value for coordinates
     */
    double getDouble(int rowIndex, int colIndex);

    /**
     * Returns the value for the coordinates specified
     * @param row       the row key coordinate
     * @param column    the column key coordinate
     * @return  the value for coordinates
     */
    double getDouble(R row, C column);

    /**
     * Returns the value for the coordinates specified
     * @param row       the row key coordinate
     * @param colIndex  the column index coordinate
     * @return  the value for coordinates
     */
    double getDouble(R row, int colIndex);

    /**
     * Returns the value for the coordinates specified
     * @param rowIndex  the row index coordinate
     * @param column    the column key coordinate
     * @return  the value for coordinates
     */
    double getDouble(int rowIndex, C column);

    /**
     * Returns the value for the coordinates specified
     * @param rowIndex  the row index coordinate
     * @param colIndex  the column index coordinate
     * @return  the value for coordinates
     */
    <T> T getValue(int rowIndex, int colIndex);

    /**
     * Returns the value for the coordinates specified
     * @param row       the row key coordinate
     * @param column    the column key coordinate
     * @return  the value for coordinates
     */
    <T> T getValue(R row, C column);

    /**
     * Returns the value for the coordinates specified
     * @param row       the row key coordinate
     * @param colIndex  the column index coordinate
     * @return  the value for coordinates
     */
    <T> T getValue(R row, int colIndex);

    /**
     * Returns the value for the coordinates specified
     * @param rowIndex  the row index coordinate
     * @param column    the column key coordinate
     * @return  the value for coordinates
     */
    <T> T getValue(int rowIndex, C column);

    /**
     * Sets the value at the coordinates specified
     * @param rowIndex  the row index coordinate
     * @param colIndex  the column index coordinate
     * @param value  the value to set
     */
    boolean setBoolean(int rowIndex, int colIndex, boolean value);

    /**
     * Returns the value for the coordinates specified
     * @param row       the row key coordinate
     * @param column    the column key coordinate
     * @param value  the value to set
     */
    boolean setBoolean(R row, C column, boolean value);

    /**
     * Returns the value for the coordinates specified
     * @param row       the row key coordinate
     * @param colIndex  the column index coordinate
     * @param value  the value to set
     */
    boolean setBoolean(R row, int colIndex, boolean value);

    /**
     * Returns the value for the coordinates specified
     * @param rowIndex  the row index coordinate
     * @param column    the column key coordinate
     * @param value  the value to set
     */
    boolean setBoolean(int rowIndex, C column, boolean value);

    /**
     * Sets the value at the coordinates specified
     * @param rowIndex  the row index coordinate
     * @param colIndex  the column index coordinate
     * @param value  the value to set
     */
    int setInt(int rowIndex, int colIndex, int value);

    /**
     * Returns the value for the coordinates specified
     * @param row       the row key coordinate
     * @param column    the column key coordinate
     * @param value  the value to set
     */
    int setInt(R row, C column, int value);

    /**
     * Returns the value for the coordinates specified
     * @param row       the row key coordinate
     * @param colIndex  the column index coordinate
     * @param value  the value to set
     */
    int setInt(R row, int colIndex, int value);

    /**
     * Returns the value for the coordinates specified
     * @param rowIndex  the row index coordinate
     * @param column    the column key coordinate
     * @param value  the value to set
     */
    int setInt(int rowIndex, C column, int value);

    /**
     * Sets the value at the coordinates specified
     * @param rowIndex  the row index coordinate
     * @param colIndex  the column index coordinate
     * @param value  the value to set
     */
    long setLong(int rowIndex, int colIndex, long value);

    /**
     * Returns the value for the coordinates specified
     * @param row       the row key coordinate
     * @param column    the column key coordinate
     * @param value  the value to set
     */
    long setLong(R row, C column, long value);

    /**
     * Returns the value for the coordinates specified
     * @param row       the row key coordinate
     * @param colIndex  the column index coordinate
     * @param value  the value to set
     */
    long setLong(R row, int colIndex, long value);

    /**
     * Returns the value for the coordinates specified
     * @param rowIndex  the row index coordinate
     * @param column    the column key coordinate
     * @param value  the value to set
     */
    long setLong(int rowIndex, C column, long value);

    /**
     * Sets the value at the coordinates specified
     * @param rowIndex  the row index coordinate
     * @param colIndex  the column index coordinate
     * @param value  the value to set
     */
    double setDouble(int rowIndex, int colIndex, double value);

    /**
     * Returns the value for the coordinates specified
     * @param row       the row key coordinate
     * @param column    the column key coordinate
     * @param value  the value to set
     */
    double setDouble(R row, C column, double value);

    /**
     * Returns the value for the coordinates specified
     * @param row       the row key coordinate
     * @param colIndex  the column index coordinate
     * @param value  the value to set
     */
    double setDouble(R row, int colIndex, double value);

    /**
     * Returns the value for the coordinates specified
     * @param rowIndex  the row index coordinate
     * @param column    the column key coordinate
     * @param value  the value to set
     */
    double setDouble(int rowIndex, C column, double value);

    /**
     * Sets the value at the coordinates specified
     * @param rowIndex  the row index coordinate
     * @param colIndex  the column index coordinate
     * @param value  the value to set
     */
    <T> T setValue(int rowIndex, int colIndex, T value);

    /**
     * Returns the value for the coordinates specified
     * @param row       the row key coordinate
     * @param column    the column key coordinate
     * @param value  the value to set
     */
    <T> T setValue(R row, C column, T value);

    /**
     * Returns the value for the coordinates specified
     * @param row       the row key coordinate
     * @param colIndex  the column index coordinate
     * @param value  the value to set
     */
    <T> T setValue(R row, int colIndex, T value);

    /**
     * Returns the value for the coordinates specified
     * @param rowIndex  the row index coordinate
     * @param column    the column key coordinate
     * @param value  the value to set
     */
    <T> T setValue(int rowIndex, C column, T value);


    /**
     * An movable vector interface onto this content
     * A Vector is movable and is therefore not thread safe so concurrent access needs to be managed.
     *
     * @param <X>   the row or column key type, if this represents a row or column
     * @param <Y>   the column or row key type, if this represents a column or row
     */
    interface Vector<X,Y> {

        /**
         * Retuns the row or column key for this vector
         * @return  the row or column key
         */
        X key();

        /**
         * Returns the row or column ordinal for this vector
         * @return  the row or column ordinal
         */
        int ordinal();

        /**
         * Moves this vector to the key specified
         * @param key   the key to relocate this vector view to
         * @return      this vector reference
         */
        Vector<X,Y> moveTo(X key);

        /**
         * Moves this vector to the ordinal specified
         * @param ordinal   the ordinal to relocate this vector to
         * @return          this vector reference
         */
        Vector<X,Y> moveTo(int ordinal);

        /**
         * Returns true if this vector represents a numeric type
         * @return      true if this vector represents a numeric type
         */
        boolean isNumeric();

        /**
         * Returns the type info for this vector
         * @return      the type info
         */
        Class<?> typeInfo();

        /**
         * Returns the value in this vector for the key specified
         * @param key   the row or column key if this represents a column or row respectively
         * @return      the value for key
         */
        boolean getBoolean(Y key);

        /**
         * Returns the value in this vector for the ordinal specified
         * @param ordinal   the row or column ordinal if this represents a column or row respectively
         * @return      the value for key
         */
        boolean getBoolean(int ordinal);

        /**
         * Returns the value in this vector for the key specified
         * @param key   the row or column key if this represents a column or row respectively
         * @return      the value for key
         */
        int getInt(Y key);

        /**
         * Returns the value in this vector for the ordinal specified
         * @param ordinal   the row or column ordinal if this represents a column or row respectively
         * @return      the value for key
         */
        int getInt(int ordinal);

        /**
         * Returns the value in this vector for the key specified
         * @param key   the row or column key if this represents a column or row respectively
         * @return      the value for key
         */
        long getLong(Y key);

        /**
         * Returns the value in this vector for the ordinal specified
         * @param ordinal   the row or column ordinal if this represents a column or row respectively
         * @return      the value for key
         */
        long getLong(int ordinal);

        /**
         * Returns the value in this vector for the key specified
         * @param key   the row or column key if this represents a column or row respectively
         * @return      the value for key
         */
        double getDouble(Y key);

        /**
         * Returns the value in this vector for the ordinal specified
         * @param ordinal   the row or column ordinal if this represents a column or row respectively
         * @return      the value for key
         */
        double getDouble(int ordinal);

        /**
         * Returns the value in this vector for the key specified
         * @param key   the row or column key if this represents a column or row respectively
         * @return      the value for key
         */
        <V> V getValue(Y key);

        /**
         * Returns the value in this vector for the ordinal specified
         * @param ordinal   the row or column ordinal if this represents a column or row respectively
         * @return      the value for key
         */
        <V> V getValue(int ordinal);

        /**
         * Sets the value in this vector for the key specified
         * @param key   the row or column key if this represents a column or row respectively
         * @param value the value to set
         * @return      the previous value
         */
        boolean setBoolean(Y key, boolean value);

        /**
         * Sets the value in this vector for the ordinal specified
         * @param ordinal   the row or column ordinal if this represents a column or row respectively
         * @param value     the value to set
         * @return          the previous value
         */
        boolean setBoolean(int ordinal, boolean value);

        /**
         * Sets the value in this vector for the key specified
         * @param key   the row or column key if this represents a column or row respectively
         * @param value the value to set
         * @return      the previous value
         */
        int setInt(Y key, int value);

        /**
         * Sets the value in this vector for the ordinal specified
         * @param ordinal   the row or column ordinal if this represents a column or row respectively
         * @param value     the value to set
         * @return          the previous value
         */
        int setInt(int ordinal, int value);

        /**
         * Sets the value in this vector for the key specified
         * @param key   the row or column key if this represents a column or row respectively
         * @param value the value to set
         * @return      the previous value
         */
        long setLong(Y key, long value);

        /**
         * Sets the value in this vector for the ordinal specified
         * @param ordinal   the row or column ordinal if this represents a column or row respectively
         * @param value     the value to set
         * @return          the previous value
         */
        long setLong(int ordinal, long value);

        /**
         * Sets the value in this vector for the key specified
         * @param key   the row or column key if this represents a column or row respectively
         * @param value the value to set
         * @return      the previous value
         */
        double setDouble(Y key, double value);

        /**
         * Sets the value in this vector for the ordinal specified
         * @param ordinal   the row or column ordinal if this represents a column or row respectively
         * @param value     the value to set
         * @return          the previous value
         */
        double setDouble(int ordinal, double value);

        /**
         * Sets the value in this vector for the key specified
         * @param key   the row or column key if this represents a column or row respectively
         * @param value the value to set
         * @return      the previous value
         */
        <V> V setValue(Y key, V value);

        /**
         * Sets the value in this vector for the ordinal specified
         * @param ordinal   the row or column ordinal if this represents a column or row respectively
         * @param value     the value to set
         * @return          the previous value
         */
        <V> V setValue(int ordinal, V value);

    }

}
