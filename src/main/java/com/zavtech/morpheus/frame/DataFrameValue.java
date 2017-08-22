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

import java.util.Comparator;

/**
 * An interface to a value within a <code>DataFrame</code> which is aware of its location
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameValue<R,C> extends Comparable<DataFrameValue<R,C>> {

    /**
     * Returns row key for this value
     * @return  the row key for value
     */
    R rowKey();

    /**
     * Returns the column key for this value
     * @return  the column key for value
     */
    C colKey();

    /**
     * Returns the row ordinal position for this value
     * @return  the row ordinal
     */
    int rowOrdinal();

    /**
     * Returns the column ordinal position for this value
     * @return  the column ordinal
     */
    int colOrdinal();

    /**
     * Reurns true if this value contains a null value
     * @return  true if value is null
     */
    boolean isNull();

    /**
     * Returns true if the internal value equals the argument
     * @param value the argument to check for equality
     * @return      true if equal, false otherwise
     */
    boolean isEqualTo(Object value);

    /**
     * Returns true if this value holds a boolean value
     * @return  true if value is boolean, false otherwise
     */
    boolean isBoolean();

    /**
     * Returns true if this value holds a int value
     * @return  true if value is int, false otherwise
     */
    boolean isInteger();

    /**
     * Returns true if this value holds a long value
     * @return  true if value is long, false otherwise
     */
    boolean isLong();

    /**
     * Returns true if this value holds a double value
     * @return  true if value is double, false otherwise
     */
    boolean isDouble();

    /**
     * Returns true if this values holds either an int, long or double
     * @return      true if value is numeric
     */
    boolean isNumeric();

    /**
     * Returns a reference to the frame to which this value belongs
     * @return      the frame associated with value
     */
    DataFrame<R,C> frame();

    /**
     * Returns a reference to row to which this value belongs
     * @return      the row to which is value belongs
     */
    DataFrameRow<R,C> row();

    /**
     * Returns a reference to column to which this value belongs
     * @return      the column to which is value belongs
     */
    DataFrameColumn<R,C> col();

    /**
     * Returns the value for current coordinates
     * @return  the value for coordinates
     */
    boolean getBoolean();

    /**
     * Returns the value for current coordinates
     * @return  the value for coordinates
     */
    int getInt();

    /**
     * Returns the value for current coordinates
     * @return  the value for coordinates
     */
    long getLong();

    /**
     * Returns the value for current coordinates
     * @return  the value for coordinates
     */
    double getDouble();

    /**
     * Returns the value for current coordinates
     * @return  the value for coordinates
     */
    <V> V getValue();

    /**
     * Sets the value for the current element
     * @param value the value to set
     */
    void setBoolean(boolean value);

    /**
     * Sets the value for the current element
     * @param value the value to set
     */
    void setInt(int value);

    /**
     * Sets the value for the current element
     * @param value the value to set
     */
    void setLong(long value);

    /**
     * Sets the value for the current element
     * @param value the value to set
     */
    void setDouble(double value);

    /**
     * Sets the value for the current element
     * @param value the value to set
     */
    <V> void setValue(V value);

    /**
     * Returns a comparator to compare DataFrameValues based on their getDouble() method
     * @param <R>   the row key type
     * @param <C>   the column key type
     * @return      the comparator
     */
    static <R,C> Comparator<DataFrameValue<R,C>> doubleComparator() {
        return (v1, v2) -> Double.compare(v1.getDouble(), v2.getDouble());
    }

}
