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
package com.zavtech.morpheus.array;

/**
 * An interface to an Array value that provides access to the index and the value
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public interface ArrayValue<T> extends Cloneable {

    /**
     * Returns a reference to the array this value belongs to
     * @return      the array reference
     */
    Array<T> array();

    /**
     * Returns the array index for this value
     * @return  the array index
     */
    int index();

    /**
     * Returns the value for current index
     * @return  the value for index
     */
    boolean getBoolean();

    /**
     * Returns the value for current index
     * @return  the value for index
     */
    int getInt();

    /**
     * Returns the value for current index
     * @return  the value for index
     */
    long getLong();

    /**
     * Returns the value for current index
     * @return  the value for index
     */
    double getDouble();

    /**
     * Returns the value for current index
     * @return  the value for index
     */
    T getValue();

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
    void setValue(T value);

    /**
     * Returns true if this value represents null
     * @return  true if the value represents a null
     */
    boolean isNull();

    /**
     * Returns true if this contains a value that matches the argument
     * @param value the value to check for equality
     * @return      true if the internal value matches the value specified
     */
    boolean isEqualTo(T value);

}
