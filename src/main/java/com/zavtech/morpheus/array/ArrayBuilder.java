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

import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.util.Asserts;

/**
 * A class designed to build an array incrementally, without necessarily knowing the type upfront, or the final length.
 *
 * @param <T>   the array element typeCode
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class ArrayBuilder<T> {

    private int length;
    private int index = 0;
    private Class<T> type;
    private Array<T> array;
    private ArrayType typeCode;
    private boolean checkType;

    /**
     * Constructor
     * @param initialLength the initial length for array to build
     * @param type  the optional array element typeCode if known (null allowed)
     * @param defaultValue  the default value for the array (null allowed, even for primitive types)
     * @param loadFactor    the array load factor which must be > 0 and <= 1 (1 implies dense array, < 1 implies spare array)
     */
    @SuppressWarnings("unchecked")
    private ArrayBuilder(int initialLength, Class<T> type, T defaultValue, float loadFactor) {
        Asserts.check(loadFactor > 0f, "The load factor mus be > 0 and <= 1");
        Asserts.check(loadFactor <= 1f, "The load factor mus be > 0 and <= 1");
        this.length = initialLength > 0 ? initialLength : 10;
        if (type != null) {
            this.type = type;
            this.typeCode = ArrayType.of(type);
            this.array = Array.of(type, length, defaultValue, loadFactor);
            this.checkType = false;
        } else if (defaultValue != null) {
            this.type = (Class<T>)defaultValue.getClass();
            this.typeCode = ArrayType.of(this.type);
            this.array = Array.of(this.type, length, defaultValue, loadFactor);
            this.checkType = false;
        }
    }

    /**
     * Returns a newly created builder for a dense arrays with initial length
     * @param initialLength     the initial capacity for builder
     * @param <T>               the array element typeCode
     * @return                  the newly created builder
     */
    public static <T> ArrayBuilder<T> of(int initialLength) {
        return new ArrayBuilder<>(initialLength, null, null, 1f);
    }

    /**
     * Returns a newly created builder for a dense arrays based on the arguments provided
     * @param initialLength     the initial capacity for builder
     * @param type              the typeCode for array elements
     * @param <T>               the array element typeCode
     * @return                  the newly created builder
     */
    public static <T> ArrayBuilder<T> of(int initialLength, Class<T> type) {
        return new ArrayBuilder<>(initialLength, type, null, 1f);
    }

    /**
     * Returns a newly created builder for a dense arrays based on the arguments provided
     * @param initialLength     the initial capacity for builder
     * @param type              the typeCode for array elements
     * @param defaultValue      the default value for the array (null allowed, even for primitive types)
     * @param <T>               the array element typeCode
     * @return                  the newly created builder
     */
    public static <T> ArrayBuilder<T> of(int initialLength, Class<T> type, T defaultValue) {
        return new ArrayBuilder<>(initialLength, type, defaultValue, 1f);
    }

    /**
     * Returns a newly created builder for a dense arrays based on the arguments provided
     * @param initialLength     the initial capacity for builder
     * @param type              the typeCode for array elements
     * @param defaultValue      the default value for the array (null allowed, even for primitive types)
     * @param loadFactor        the array load factor which must be > 0 and <= 1 (1 implies dense array, < 1 implies spare array)
     * @param <T>               the array element typeCode
     * @return                  the newly created builder
     */
    public static <T> ArrayBuilder<T> of(int initialLength, Class<T> type, T defaultValue, float loadFactor) {
        return new ArrayBuilder<>(initialLength, type, defaultValue, loadFactor);
    }

    /**
     * Adds an entry to array being built
     * @param value the value to add
     * @return  the index assigned to element
     */
    @SuppressWarnings("unchecked")
    public final int add(T value) {
        if (value != null) {
            this.checkType((Class<T>)value.getClass());
            this.checkLength();
            this.array.setValue(index, value);
        }
        this.index++;
        return index-1;
    }

    /**
     * Adds an entry to array being built
     * @param value the value to add
     * @return  the index assigned to element
     */
    @SuppressWarnings("unchecked")
    public final int addBoolean(boolean value) {
        this.checkType((Class<T>)Boolean.class);
        this.checkLength();
        this.array.setBoolean(index++, value);
        return index-1;
    }

    /**
     * Adds an entry to array being built
     * @param value the value to add
     * @return  the index assigned to element
     */
    @SuppressWarnings("unchecked")
    public final int addInt(int value) {
        this.checkType((Class<T>)Integer.class);
        this.checkLength();
        this.array.setInt(index++, value);
        return index-1;
    }

    /**
     * Adds an entry to array being built
     * @param value the value to add
     * @return  the index assigned to element
     */
    @SuppressWarnings("unchecked")
    public final int addLong(long value) {
        this.checkType((Class<T>)Long.class);
        this.checkLength();
        this.array.setLong(index++, value);
        return index-1;
    }

    /**
     * Adds an entry to array being built
     * @param value the value to add
     * @return  the index assigned to element
     */
    @SuppressWarnings("unchecked")
    public final int addDouble(double value) {
        this.checkType((Class<T>)Double.class);
        this.checkLength();
        this.array.setDouble(index++, value);
        return index-1;
    }

    /**
     * Adds all values from the iterable specified
     * @param values    the values to add
     */
    public final ArrayBuilder<T> addAll(Iterable<T> values) {
        if (values instanceof Range) {
            values.forEach(this::add);
        } else if (values instanceof Array) {
            final Array<T> newArray = (Array<T>)values;
            final int newLength = index + newArray.length();
            this.array.expand(newLength);
            switch (newArray.typeCode()) {
                case BOOLEAN:           newArray.forEachBoolean(this::addBoolean);  break;
                case INTEGER:           newArray.forEachInt(this::addInt);          break;
                case LONG:              newArray.forEachLong(this::addLong);        break;
                case DOUBLE:            newArray.forEachDouble(this::addDouble);    break;
                case DATE:              newArray.forEachLong(this::addLong);        break;
                case INSTANT:           newArray.forEachLong(this::addLong);        break;
                case LOCAL_DATE:        newArray.forEachLong(this::addLong);        break;
                case LOCAL_TIME:        newArray.forEachLong(this::addLong);        break;
                case LOCAL_DATETIME:    newArray.forEachLong(this::addLong);        break;
                default:                newArray.forEach(this::add);                break;
            }
        } else {
            values.forEach(this::add);
        }
        return this;
    }

    /**
     * Checks that the specified data typeCode is compatible with the current array we are building
     * @param dataType  the element data typeCode being added
     */
    @SuppressWarnings("unchecked")
    private void checkType(Class<T> dataType) {
        if (array == null) {
            this.type = dataType;
            this.typeCode = ArrayType.of(dataType);
            this.array = Array.of(dataType, length);
            this.checkType = typeCode != ArrayType.OBJECT;
            this.length = array.length();
        } else if (checkType && !isMatch(dataType)) {
            final Array<T> newArray = Array.ofObjects(array.length());
            for (int i=0; i<array.length(); ++i) newArray.setValue(i, array.getValue(i));
            this.array = newArray;
            this.type = (Class<T>)Object.class;
            this.typeCode = ArrayType.OBJECT;
            this.length = array.length();
        }
    }

    /**
     * Returns true if the data typeCode is a match for the current array typeCode
     * @param dataType  the data typeCode class
     * @return          true if match
     */
    private boolean isMatch(Class<T> dataType) {
        return dataType == type || type.isAssignableFrom(dataType);
    }

    /**
     * Checks to see if the current length supports adding another value, expanding if necessary
     */
    private void checkLength() {
        if (index >= length) {
            int newLength = length + (length >> 1);
            if (newLength < index + 1) newLength = index + 1;
            this.array.expand(newLength);
            this.length = array.length();
        }
    }

    /**
     * Adds all items from the other builder
     * @param other the other builder
     */
    public final ArrayBuilder<T> addAll(ArrayBuilder<T> other) {
        if (array == null) {
            this.array = other.array.copy();
            return this;
        } else {
            final Array<T> arrayToAdd = other.array;
            final int totalLength = this.index + other.index;
            if (totalLength > array.length()) array.expand(totalLength);
            for (int i=0; i<other.index; ++i) {
                //todo: optimize this for common types to avoid boxing
                final T value = arrayToAdd.getValue(i);
                this.add(value);
            }
            return this;
        }
    }

    /**
     * Returns the final array for this appender
     * @return  the final array result
     */
    @SuppressWarnings("unchecked")
    public final Array<T> toArray() {
        if (array == null) {
            return Array.ofObjects(0);
        } else {
            return index < array.length() ? array.copy(0, index) : array;
        }
    }

}
